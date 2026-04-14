# Job Crawler: Technical Deep-Dive
## Models, Features, Optimization Strategies & Prompts

---

## 1. The Two ML Problems, Precisely Defined

Before touching a model, it helps to be precise about what you're actually solving.

**Task 1 — Binary Page Classifier**
Input: (url: str, html: str) → Output: P(is_career_page) ∈ [0,1]

This is a well-behaved binary classification problem. Class imbalance will be severe (maybe 1–3% of crawled pages are careers pages), so you need to weight this carefully. The signal is rich, the features are interpretable, and LightGBM will likely dominate.

**Task 2 — Structured Extraction**
Input: (html: str, confirmed_careers_page: bool) → Output: list[JobRecord]

This is actually *three* sub-problems:
- (a) Detect which ATS/platform pattern is present (multi-class classifier)
- (b) If no ATS match, identify the job listing region in the DOM (sequence/structure problem)
- (c) Extract named fields from each listing (sequence labeling / NLP extraction)

These need different techniques. Don't treat it as one problem.

---

## 2. Feature Engineering — The Real Leverage Point

Feature engineering is where you'll get the most performance gains in a classical ML system. Here's the full breakdown.

### 2a. URL Features

These are cheap to compute and surprisingly powerful. Path tokens alone can get you to ~0.80 F1 before looking at any page content.

```python
JOB_TOKENS = {
    'careers', 'career', 'jobs', 'job', 'work', 'working',
    'positions', 'position', 'openings', 'opening', 'vacancies',
    'vacancy', 'hiring', 'recruit', 'recruitment', 'apply',
    'join', 'opportunities', 'opportunity', 'employment',
    'werkenbij', 'stellenangebote', 'emplois', 'empleo'  # multilingual
}

NEGATIVE_TOKENS = {
    'blog', 'news', 'press', 'media', 'about', 'contact',
    'support', 'help', 'faq', 'privacy', 'terms', 'legal'
}

def extract_url_features(url: str) -> dict:
    parsed = urlparse(url)
    path_tokens = set(re.split(r'[/\-_]', parsed.path.lower()))
    
    return {
        # Token signals
        'url_job_token_count': len(path_tokens & JOB_TOKENS),
        'url_negative_token_count': len(path_tokens & NEGATIVE_TOKENS),
        'url_has_primary_job_token': int(bool({'jobs','careers','positions'} & path_tokens)),
        
        # Structure signals
        'url_path_depth': parsed.path.count('/'),
        'url_path_length': len(parsed.path),
        'url_has_query_params': int(bool(parsed.query)),
        'url_has_hash': int(bool(parsed.fragment)),
        'url_is_subdomain': int(parsed.netloc.count('.') > 1),
        
        # ATS signals (known ATS domains/paths that indicate job listing pages)
        'url_is_greenhouse': int('greenhouse.io' in url or '/jobs/' in url),
        'url_is_lever': int('lever.co' in url),
        'url_is_workday': int('myworkdayjobs.com' in url),
        'url_is_icims': int('icims.com' in url),
        'url_is_smartrecruiters': int('smartrecruiters.com' in url),
        'url_is_ashby': int('ashbyhq.com' in url),
        'url_is_rippling': int('rippling.com' in url and '/jobs' in url),
        'url_is_breezy': int('breezy.hr' in url),
        'url_is_jobvite': int('jobvite.com' in url),
        'url_is_taleo': int('taleo.net' in url),
        'url_is_successfactors': int('successfactors.com' in url),
        
        # Combined ATS signal
        'url_is_known_ats': int(any(ats in url for ats in [
            'greenhouse.io','lever.co','myworkdayjobs.com','icims.com',
            'smartrecruiters.com','ashbyhq.com','breezy.hr','jobvite.com',
            'taleo.net','successfactors.com','bamboohr.com'
        ])),
    }
```

### 2b. HTML Content Features

```python
APPLY_PATTERNS = re.compile(
    r'\b(apply now|apply for|apply online|submit application|'
    r'apply today|apply here|apply to this|start application)\b',
    re.IGNORECASE
)

JOB_FIELD_PATTERNS = {
    'location': re.compile(r'\b(location|where|city|remote|hybrid|on-site)\b', re.I),
    'salary': re.compile(r'\$[\d,]+|\d+k\s*(usd|aud|gbp|eur)?|salary|compensation|pay range', re.I),
    'type': re.compile(r'\b(full.time|part.time|contract|permanent|casual|freelance)\b', re.I),
    'department': re.compile(r'\b(engineering|marketing|sales|design|product|finance|hr|legal)\b', re.I),
}

def extract_content_features(html: str) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    
    # Remove noise
    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
        tag.decompose()
    
    title = soup.find('title')
    title_text = title.get_text() if title else ''
    body_text = soup.get_text(separator=' ', strip=True)
    
    # Headings
    h1_texts = [h.get_text() for h in soup.find_all('h1')]
    h2_texts = [h.get_text() for h in soup.find_all('h2')]
    heading_text = ' '.join(h1_texts + h2_texts).lower()
    
    # Schema.org
    json_ld_blocks = soup.find_all('script', type='application/ld+json')
    has_job_schema = False
    job_schema_count = 0
    for block in json_ld_blocks:
        try:
            data = json.loads(block.string or '')
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get('@type') in ('JobPosting', 'JobListing'):
                    has_job_schema = True
                    job_schema_count += 1
        except (json.JSONDecodeError, AttributeError):
            pass
    
    # Link analysis
    all_links = soup.find_all('a', href=True)
    link_texts = [a.get_text(strip=True).lower() for a in all_links]
    
    # Repeated structure detection (key signal for listing pages)
    # Count structurally similar siblings with a minimum text density
    def count_repeated_structures(soup, min_count=3):
        candidates = 0
        for parent in soup.find_all(True):
            children = [c for c in parent.children if hasattr(c, 'name') and c.name]
            if len(children) >= min_count:
                tag_names = [c.name for c in children]
                most_common = max(set(tag_names), key=tag_names.count)
                count = tag_names.count(most_common)
                if count >= min_count and most_common in ('li', 'div', 'article', 'tr'):
                    text_lengths = [len(c.get_text(strip=True)) for c in children 
                                  if c.name == most_common]
                    # Similar-length text = likely job listings
                    if len(text_lengths) >= 3:
                        avg = sum(text_lengths) / len(text_lengths)
                        if avg > 30:  # meaningful text, not just nav items
                            candidates += count
        return candidates
    
    repeated_count = count_repeated_structures(soup)
    
    # Word counts
    words = body_text.lower().split()
    word_count = len(words)
    job_term_count = sum(1 for w in words if w in JOB_TOKENS)
    
    return {
        # Schema signals
        'has_job_schema': int(has_job_schema),
        'job_schema_count': job_schema_count,
        
        # Title signals  
        'title_has_job_keyword': int(bool(re.search(r'\b(jobs|careers|positions|openings)\b', title_text, re.I))),
        'title_length': len(title_text),
        
        # Heading signals
        'heading_has_job_keyword': int(bool(re.search(r'\b(jobs|careers|positions|openings|roles|opportunities)\b', heading_text))),
        'h1_count': len(h1_texts),
        
        # Apply signals
        'has_apply_button': int(bool(APPLY_PATTERNS.search(body_text))),
        'apply_link_count': sum(1 for t in link_texts if re.search(r'\bapply\b', t)),
        
        # Structure signals
        'repeated_structure_count': repeated_count,
        'estimated_listing_count': min(repeated_count, 200),
        
        # Content density signals
        'word_count': word_count,
        'job_term_density': job_term_count / max(word_count, 1),
        'job_term_count': job_term_count,
        
        # Field presence signals
        'has_location_mentions': int(bool(JOB_FIELD_PATTERNS['location'].search(body_text))),
        'has_salary_mentions': int(bool(JOB_FIELD_PATTERNS['salary'].search(body_text))),
        'has_job_type_mentions': int(bool(JOB_FIELD_PATTERNS['type'].search(body_text))),
        'has_department_mentions': int(bool(JOB_FIELD_PATTERNS['department'].search(body_text))),
        
        # Link signals
        'total_link_count': len(all_links),
        'job_keyword_link_count': sum(1 for t in link_texts if any(kw in t for kw in ['job', 'career', 'role', 'position'])),
        
        # Meta signals
        'has_meta_keywords': int(bool(soup.find('meta', attrs={'name': 'keywords'}))),
        'page_text_length': len(body_text),
    }
```

### 2c. Embedding Features

These capture semantic similarity that rule-based features miss. Use `all-MiniLM-L6-v2` as your workhorse — it's fast (CPU-friendly), small (80MB), and performs well on web text.

```python
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')

# Reference texts that represent "career pages" in semantic space
CAREER_PAGE_REFERENCES = [
    "Current job openings and career opportunities at our company",
    "Join our team. Browse open positions and apply today.",
    "We are hiring. View all available roles and departments.",
    "Work with us. Explore careers and submit your application.",
    "Find your next role. Filter by location, team, and experience level.",
]

# Pre-compute reference embedding centroid (do this once at startup)
CAREER_CENTROID = model.encode(CAREER_PAGE_REFERENCES).mean(axis=0)

def extract_embedding_features(html: str, pca: PCA = None) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup(['script', 'style', 'nav', 'footer']):
        tag.decompose()
    
    title = (soup.find('title') or soup.find('h1') or soup.new_tag('span')).get_text()
    body_words = soup.get_text(separator=' ', strip=True).split()[:200]
    text_to_embed = f"{title} {' '.join(body_words)}"
    
    embedding = model.encode(text_to_embed)  # 384-dim
    
    # Cosine similarity to career page centroid (single strong signal)
    cos_sim = np.dot(embedding, CAREER_CENTROID) / (
        np.linalg.norm(embedding) * np.linalg.norm(CAREER_CENTROID) + 1e-8
    )
    
    features = {
        'embedding_career_similarity': float(cos_sim),
    }
    
    # If PCA is fitted (after initial training), add reduced embedding dims
    if pca is not None:
        reduced = pca.transform(embedding.reshape(1, -1))[0]
        for i, v in enumerate(reduced):
            features[f'emb_{i}'] = float(v)
    
    return features
```

**PCA strategy:** Fit PCA on your training set embeddings, keep 30–50 components (typically captures 85–90% variance), then use those components as features in your tree model. This is much better than feeding 384 raw floats to LightGBM.

---

## 3. Task 1: Career Page Classifier — Model Selection

### Why LightGBM wins here

- Handles class imbalance natively via `scale_pos_weight`
- Fast training — can retrain from scratch in seconds on 100K examples
- Built-in feature importance (critical for the improvement loop)
- Handles mixed feature types (binary flags + continuous + embeddings)
- Categorical support (`num_leaves`, `min_child_samples` prevent overfitting on small feature groups)
- Native missing value handling (important when some pages fail to parse)

```python
import lightgbm as lgb
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import classification_report, average_precision_score
import optuna

class CareerPageClassifier:
    
    DEFAULT_PARAMS = {
        'objective': 'binary',
        'metric': ['binary_logloss', 'auc'],
        'boosting_type': 'gbdt',
        'num_leaves': 63,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'min_child_samples': 20,
        'reg_alpha': 0.1,
        'reg_lambda': 0.1,
        'verbose': -1,
    }
    
    def fit(self, X: pd.DataFrame, y: np.ndarray, params: dict = None, 
            n_rounds: int = 500) -> 'CareerPageClassifier':
        
        # Class imbalance: weight positives more
        pos_count = y.sum()
        neg_count = len(y) - pos_count
        scale_pos_weight = neg_count / pos_count
        
        p = {**self.DEFAULT_PARAMS, 'scale_pos_weight': scale_pos_weight}
        if params:
            p.update(params)
        
        # Cross-validation to find best n_rounds, then train on full data
        cv_data = lgb.Dataset(X, label=y)
        cv_result = lgb.cv(
            p, cv_data, num_boost_round=n_rounds,
            nfold=5, stratified=True,
            callbacks=[lgb.early_stopping(50, verbose=False)],
            return_cvbooster=False
        )
        best_round = len(cv_result['valid binary_logloss-mean'])
        
        train_data = lgb.Dataset(X, label=y)
        self.model = lgb.train(p, train_data, num_boost_round=best_round)
        self.feature_names = list(X.columns)
        self.best_round = best_round
        self.threshold = 0.5  # Can be tuned via precision-recall curve
        
        return self
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X)
    
    def get_feature_importance(self) -> pd.DataFrame:
        return pd.DataFrame({
            'feature': self.feature_names,
            'gain': self.model.feature_importance(importance_type='gain'),
            'split': self.model.feature_importance(importance_type='split'),
        }).sort_values('gain', ascending=False)
    
    def tune_threshold(self, X_val: pd.DataFrame, y_val: np.ndarray,
                       optimize_for: str = 'f1') -> float:
        """Find optimal classification threshold on validation data."""
        from sklearn.metrics import f1_score, precision_score, recall_score
        probas = self.predict_proba(X_val)
        thresholds = np.arange(0.1, 0.9, 0.02)
        
        if optimize_for == 'f1':
            scores = [f1_score(y_val, probas >= t) for t in thresholds]
        elif optimize_for == 'precision':
            scores = [precision_score(y_val, probas >= t, zero_division=0) for t in thresholds]
        
        self.threshold = thresholds[np.argmax(scores)]
        return self.threshold
```

### Hyperparameter Optimization with Optuna

Run this as a challenger generation strategy — each Optuna study IS a challenger:

```python
def create_lgbm_challenger(X_train, y_train, X_val, y_val, n_trials=50) -> dict:
    """Run an Optuna study and return the best hyperparameter config."""
    
    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'average_precision',
            'verbosity': -1,
            'boosting_type': trial.suggest_categorical('boosting_type', ['gbdt', 'dart', 'goss']),
            'num_leaves': trial.suggest_int('num_leaves', 20, 300),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.3, log=True),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-4, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-4, 10.0, log=True),
            'scale_pos_weight': (y_train == 0).sum() / (y_train == 1).sum(),
        }
        
        dtrain = lgb.Dataset(X_train, label=y_train)
        dval = lgb.Dataset(X_val, label=y_val, reference=dtrain)
        
        model = lgb.train(
            params, dtrain, num_boost_round=500,
            valid_sets=[dval],
            callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(-1)]
        )
        
        preds = model.predict(X_val)
        return average_precision_score(y_val, preds)
    
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, n_jobs=-1)
    
    return study.best_params
```

### Ensemble Strategy (Late-Stage Champion)

Once you have several good models (say version 4+), try a stacking ensemble as a challenger:

```python
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import StackingClassifier

# Level-0 models (diverse to reduce correlation)
estimators = [
    ('lgbm_v5', lgbm_model_v5),
    ('lgbm_v6', lgbm_model_v6),  # different hyperparams
    ('rf', RandomForestClassifier(n_estimators=200, n_jobs=-1)),
    ('svm', SVC(kernel='rbf', probability=True, C=10)),
]

# Level-1 meta-learner
stacker = StackingClassifier(
    estimators=estimators,
    final_estimator=LogisticRegression(C=1.0),
    cv=5,
    stack_method='predict_proba',
    passthrough=True,  # pass original features to meta-learner too
)
```

---

## 4. Task 2: Job Extraction — The Technical Stack

### 4a. ATS Fingerprinting (Highest ROI First)

Before any ML, pattern-match known ATS platforms. These cover 60–70% of job pages on established company sites.

```python
ATS_PATTERNS = {
    'greenhouse': {
        'url_patterns': [r'greenhouse\.io', r'\.greenhouse\.io'],
        'html_patterns': [r'id=["\']grnhse_app["\']', r'class=["\'][\w\s]*greenhouse[\w\s]*["\']'],
        'selectors': {
            'job_list': '.opening',
            'title': '.opening a',
            'department': '.department',
            'location': '.location',
        },
        'pagination': {'type': 'none'},  # Greenhouse renders all jobs on one page
    },
    'lever': {
        'url_patterns': [r'jobs\.lever\.co'],
        'selectors': {
            'job_list': '.posting',
            'title': '.posting-name h5',
            'team': '.posting-categories .sort-by-team',
            'location': '.posting-categories .sort-by-location',
            'apply_url': '.posting-apply a',
        },
    },
    'workday': {
        'url_patterns': [r'myworkdayjobs\.com'],
        'note': 'JS-heavy, requires Playwright. Look for __wd_app_info in page source.',
        'selectors': {
            'job_list': '[data-automation-id="jobItem"]',
            'title': '[data-automation-id="jobTitle"]',
            'location': '[data-automation-id="locations"]',
        },
    },
    'ashby': {
        'url_patterns': [r'ashbyhq\.com', r'jobs\.ashbyhq\.com'],
        'html_patterns': [r'"ashby_jid"'],
        'selectors': {
            'job_list': '[class*="job-board-job-posting"]',
            'title': '[class*="ashby-job-posting-brief-title"]',
            'location': '[class*="ashby-job-posting-brief-location"]',
        },
    },
    'bamboohr': {
        'url_patterns': [r'bamboohr\.com/careers'],
        'selectors': {
            'job_list': '.ResJobList-jobListing',
            'title': '.ResJobListing-title',
            'location': '.ResJobListing-location',
            'department': '.ResJobListing-department',
        },
    },
    # ... add 15 more
}

class ATSExtractor:
    
    def detect_ats(self, url: str, html: str) -> str | None:
        for ats_name, config in ATS_PATTERNS.items():
            for pattern in config.get('url_patterns', []):
                if re.search(pattern, url):
                    return ats_name
            for pattern in config.get('html_patterns', []):
                if re.search(pattern, html):
                    return ats_name
        return None
    
    def extract(self, ats_name: str, soup: BeautifulSoup, url: str) -> list[dict]:
        config = ATS_PATTERNS[ats_name]
        selectors = config['selectors']
        
        job_elements = soup.select(selectors['job_list'])
        jobs = []
        
        for el in job_elements:
            job = {'ats': ats_name, 'confidence': 0.92}
            
            for field, selector in selectors.items():
                if field in ('job_list', 'pagination'):
                    continue
                found = el.select_one(selector)
                if found:
                    job[field] = found.get_text(strip=True)
                    job[f'{field}_confidence'] = 0.92
            
            if 'apply_url' not in job:
                link = el.select_one('a[href*="apply"], a[href*="job"]')
                job['apply_url'] = urljoin(url, link['href']) if link else None
            
            if 'title' in job:  # Only add if we got at least a title
                jobs.append(job)
        
        return jobs
```

### 4b. Schema.org Extraction (Highest Confidence)

When `JobPosting` structured data exists, use it directly. This is rare but extremely reliable.

```python
def extract_schema_org(html: str) -> list[dict] | None:
    soup = BeautifulSoup(html, 'lxml')
    jobs = []
    
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else \
                    (data.get('@graph', []) if '@graph' in data else [data])
            
            for item in items:
                if item.get('@type') not in ('JobPosting',):
                    continue
                
                # Map schema.org fields to our output format
                location = item.get('jobLocation', {})
                if isinstance(location, list):
                    location = location[0] if location else {}
                address = location.get('address', {})
                
                job = {
                    'title': item.get('title'),
                    'description': item.get('description', ''),
                    'location': (
                        address.get('addressLocality') or
                        address.get('addressRegion') or
                        item.get('jobLocationType')  # TELECOMMUTE for remote
                    ),
                    'employment_type': item.get('employmentType'),
                    'salary_min': _get_nested(item, 'baseSalary.value.minValue'),
                    'salary_max': _get_nested(item, 'baseSalary.value.maxValue'),
                    'salary_currency': _get_nested(item, 'baseSalary.currency'),
                    'posted_date': item.get('datePosted'),
                    'apply_url': item.get('url') or item.get('hiringOrganization', {}).get('url'),
                    'confidence': 0.97,
                    'extraction_method': 'schema_org',
                }
                jobs.append({k: v for k, v in job.items() if v is not None})
        
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    
    return jobs if jobs else None
```

### 4c. DOM Pattern Extractor (The Hard Part)

For sites with no ATS and no structured data, learn repeating DOM patterns. This is where ML earns its place in extraction.

```python
from sklearn.cluster import DBSCAN
import numpy as np

def featurize_element(el) -> np.ndarray:
    """Convert a DOM element to a feature vector for clustering."""
    text = el.get_text(separator=' ', strip=True)
    all_text = len(text)
    links = el.find_all('a', href=True)
    
    return np.array([
        len(text),                                          # text length
        len(links),                                         # number of links
        len(el.find_all(True)),                             # total child elements
        int(bool(re.search(r'\d{4,}', text))),              # has long number (ID?)
        int(bool(re.search(r'\b(apply|apply now)\b', text, re.I))),  # has apply
        int(bool(re.search(r'\b(remote|hybrid|on.site|location)\b', text, re.I))),  # has location
        int(bool(re.search(r'\b(full.time|part.time|contract)\b', text, re.I))),   # has type
        int(bool(re.search(r'\$|\£|\€|salary|compensation', text, re.I))),         # has pay
        len(el.find_all('h1') + el.find_all('h2') + el.find_all('h3')),            # has headings
        int(el.name in ('li', 'article', 'div', 'section')),                        # likely listing
        min(all_text, 500) / 500,                           # normalized text length
    ], dtype=float)

def find_job_listing_candidates(soup: BeautifulSoup) -> list:
    """
    Identify job listing elements by finding clusters of similar DOM elements
    with job-relevant text features.
    """
    # Candidates: elements that COULD be job listings
    candidates = []
    for tag in ['li', 'article', 'div', 'tr', 'section']:
        for el in soup.find_all(tag):
            text = el.get_text(strip=True)
            if 50 < len(text) < 2000:  # Filter too short or too long
                candidates.append(el)
    
    if len(candidates) < 3:
        return []
    
    # Featurize
    features = np.array([featurize_element(el) for el in candidates])
    
    # Cluster by structural similarity
    clustering = DBSCAN(eps=0.5, min_samples=3, metric='euclidean').fit(features)
    
    # Find the cluster most likely to be job listings
    best_cluster = -1
    best_score = -1
    
    for cluster_id in set(clustering.labels_):
        if cluster_id == -1:  # noise
            continue
        
        cluster_indices = np.where(clustering.labels_ == cluster_id)[0]
        cluster_features = features[cluster_indices]
        
        # Score this cluster: prefer clusters where elements have apply links,
        # location mentions, and reasonable text length
        score = (
            cluster_features[:, 4].mean() * 3 +  # apply link presence (weight 3)
            cluster_features[:, 5].mean() * 2 +  # location presence
            cluster_features[:, 6].mean() * 1 +  # job type presence
            min(len(cluster_indices) / 10, 1.0)  # more items = more confident
        )
        
        if score > best_score:
            best_score = score
            best_cluster = cluster_id
    
    if best_cluster == -1 or best_score < 0.5:
        return []
    
    cluster_indices = np.where(clustering.labels_ == best_cluster)[0]
    return [candidates[i] for i in cluster_indices], best_score
```

### 4d. Field Extraction from a Job Element

Once you have the job listing elements, extract fields. Use a CRF (Conditional Random Field) for the sequence labeling.

```python
import sklearn_crfsuite
from sklearn_crfsuite import metrics as crf_metrics

# CRF feature extraction for a token within a job element
def token_features(tokens: list[str], i: int) -> dict:
    token = tokens[i]
    features = {
        'bias': 1.0,
        'token.lower': token.lower(),
        'token.isupper': token.isupper(),
        'token.istitle': token.istitle(),
        'token.isdigit': token.isdigit(),
        'token.length': len(token),
        
        # Prefix/suffix
        'token.prefix2': token[:2].lower(),
        'token.prefix3': token[:3].lower(),
        'token.suffix2': token[-2:].lower(),
        'token.suffix3': token[-3:].lower(),
        
        # Regex patterns
        'token.is_location_kw': bool(re.match(r'^(sydney|melbourne|london|new york|remote|hybrid)$', token, re.I)),
        'token.is_type_kw': bool(re.match(r'^(full.time|part.time|contract|permanent|casual)$', token, re.I)),
        'token.is_currency': bool(re.match(r'^\$|\£|\€', token)),
        'token.has_number': bool(re.search(r'\d', token)),
    }
    
    # Context window [-2, +2]
    for offset in [-2, -1, 1, 2]:
        pos = i + offset
        if 0 <= pos < len(tokens):
            t = tokens[pos]
            prefix = f'token{offset:+d}.'
            features.update({
                prefix + 'lower': t.lower(),
                prefix + 'istitle': t.istitle(),
                prefix + 'isupper': t.isupper(),
            })
        else:
            features[f'token{offset:+d}.BOS' if offset < 0 else f'token{offset:+d}.EOS'] = True
    
    return features

# Labels: B-TITLE, I-TITLE, B-LOC, I-LOC, B-DEPT, I-DEPT, B-TYPE, I-TYPE, O

class JobFieldCRF:
    def __init__(self):
        self.crf = sklearn_crfsuite.CRF(
            algorithm='lbfgs',
            c1=0.1,     # L1 regularization
            c2=0.1,     # L2 regularization
            max_iterations=100,
            all_possible_transitions=True
        )
    
    def fit(self, X_sequences: list[list[dict]], y_sequences: list[list[str]]):
        self.crf.fit(X_sequences, y_sequences)
    
    def extract_fields(self, text: str) -> dict:
        tokens = text.split()
        features = [token_features(tokens, i) for i in range(len(tokens))]
        labels = self.crf.predict([features])[0]
        
        # Reconstruct entities from BIO labels
        fields = {'title': [], 'location': [], 'department': [], 'type': []}
        current_entity = None
        current_tokens = []
        
        for token, label in zip(tokens, labels):
            if label.startswith('B-'):
                if current_entity:
                    fields.get(current_entity.lower(), []).append(' '.join(current_tokens))
                current_entity = label[2:]
                current_tokens = [token]
            elif label.startswith('I-') and current_entity == label[2:]:
                current_tokens.append(token)
            else:
                if current_entity:
                    fields.get(current_entity.lower(), []).append(' '.join(current_tokens))
                current_entity = None
                current_tokens = []
        
        return {k: v[0] if v else None for k, v in fields.items()}
```

---

## 5. The Champion/Challenger Optimization Loop

### Generating Challengers Systematically

The key insight is that a challenger is just a config diff from the champion. Define a challenger generation strategy:

```python
from dataclasses import dataclass, asdict
from enum import Enum
import random

class ChallengerStrategy(Enum):
    HYPERPARAM_SEARCH = "hyperparam_search"
    FEATURE_ABLATION = "feature_ablation"
    FEATURE_ADDITION = "feature_addition"
    ALGORITHM_SWAP = "algorithm_swap"
    PSEUDO_LABEL_THRESHOLD = "pseudo_label_threshold"
    EMBEDDING_MODEL_SWAP = "embedding_model_swap"
    ENSEMBLE = "ensemble"

@dataclass
class PipelineConfig:
    # Classifier
    classifier_type: str = 'lightgbm'  # 'lightgbm' | 'random_forest' | 'svm' | 'ensemble'
    classifier_params: dict = None
    
    # Features
    use_url_features: bool = True
    use_content_features: bool = True
    use_embeddings: bool = True
    use_graph_features: bool = False  # link graph depth, in-degree
    embedding_model: str = 'all-MiniLM-L6-v2'
    embedding_pca_components: int = 30
    
    # Training data
    pseudo_label_min_confidence: float = 0.80
    pseudo_label_max_age_days: int = 90
    use_augmented_negatives: bool = True  # synthesize hard negatives from near-miss pages
    
    # Thresholds
    classification_threshold: float = 0.50
    
    # Extractor
    ats_fingerprinting: bool = True
    schema_org_extraction: bool = True
    dom_pattern_extraction: bool = True
    crf_field_extraction: bool = True

def generate_challenger(champion_config: PipelineConfig, 
                        strategy: ChallengerStrategy = None) -> PipelineConfig:
    
    if strategy is None:
        strategy = random.choice(list(ChallengerStrategy))
    
    challenger = PipelineConfig(**asdict(champion_config))
    
    if strategy == ChallengerStrategy.HYPERPARAM_SEARCH:
        # Trigger an Optuna search - the resulting best params become the challenger
        challenger.classifier_params = None  # will be filled by Optuna run
        
    elif strategy == ChallengerStrategy.FEATURE_ABLATION:
        # Remove one feature group to test its marginal value
        feature_flags = ['use_url_features', 'use_content_features', 
                        'use_embeddings', 'use_graph_features']
        active = [f for f in feature_flags if getattr(challenger, f)]
        if len(active) > 1:
            remove = random.choice(active)
            setattr(challenger, remove, False)
    
    elif strategy == ChallengerStrategy.FEATURE_ADDITION:
        inactive = ['use_graph_features']  # features not yet in champion
        add = random.choice(inactive)
        setattr(challenger, add, True)
    
    elif strategy == ChallengerStrategy.ALGORITHM_SWAP:
        options = ['lightgbm', 'random_forest', 'svm']
        options.remove(challenger.classifier_type)
        challenger.classifier_type = random.choice(options)
        challenger.classifier_params = None
    
    elif strategy == ChallengerStrategy.PSEUDO_LABEL_THRESHOLD:
        # Test different confidence cutoffs for pseudo-labeling
        challenger.pseudo_label_min_confidence = random.choice([0.70, 0.75, 0.80, 0.85, 0.90, 0.95])
    
    elif strategy == ChallengerStrategy.EMBEDDING_MODEL_SWAP:
        models = ['all-MiniLM-L6-v2', 'all-mpnet-base-v2', 
                 'paraphrase-MiniLM-L6-v2', 'multi-qa-MiniLM-L6-cos-v1']
        models.remove(challenger.embedding_model)
        challenger.embedding_model = random.choice(models)
    
    return challenger, strategy
```

### Metrics and Evaluation

```python
@dataclass
class EvaluationMetrics:
    # Page classifier metrics
    precision: float
    recall: float
    f1: float
    auc_roc: float
    average_precision: float
    
    # Extraction metrics
    job_coverage_rate: float       # % of baseline jobs found
    title_accuracy: float          # exact or fuzzy title match
    location_accuracy: float
    type_accuracy: float
    
    # End-to-end
    domain_success_rate: float     # % domains where career page found AND ≥80% jobs extracted
    false_positive_rate: float     # % non-career pages misclassified
    
    # Efficiency
    pages_crawled_per_domain: float
    time_per_domain_seconds: float
    
    @property
    def primary_score(self) -> float:
        """Composite score used for champion/challenger comparison."""
        return (
            0.35 * self.f1 +
            0.25 * self.job_coverage_rate +
            0.20 * self.domain_success_rate +
            0.10 * self.title_accuracy +
            0.10 * (1 - self.false_positive_rate)
        )

def evaluate_pipeline(pipeline, holdout_domains: list) -> EvaluationMetrics:
    """Run pipeline against holdout domains and compute all metrics."""
    
    page_labels, page_preds, page_probas = [], [], []
    job_coverage_rates = []
    title_accuracies, location_accuracies = [], []
    domain_successes = []
    
    for domain in holdout_domains:
        result = pipeline.run(domain.url)
        
        # Page classifier evaluation
        for page_result in result.crawled_pages:
            page_labels.append(int(page_result.url == domain.known_career_url))
            page_preds.append(int(page_result.is_career_page_pred))
            page_probas.append(page_result.confidence)
        
        # Extraction evaluation
        if result.career_page_found:
            baseline_jobs = domain.known_jobs
            extracted_jobs = result.extracted_jobs
            
            # Job coverage (did we find roughly the right number?)
            coverage = min(len(extracted_jobs) / max(len(baseline_jobs), 1), 1.0)
            job_coverage_rates.append(coverage)
            
            # Field accuracy (fuzzy match on titles)
            matched = 0
            for baseline_job in baseline_jobs:
                best_match = max(
                    [fuzz.token_set_ratio(baseline_job.title, j.title) 
                     for j in extracted_jobs],
                    default=0
                )
                if best_match >= 80:
                    matched += 1
            
            title_accuracies.append(matched / max(len(baseline_jobs), 1))
            domain_successes.append(int(coverage >= 0.8 and matched / max(len(baseline_jobs),1) >= 0.7))
        else:
            job_coverage_rates.append(0.0)
            title_accuracies.append(0.0)
            domain_successes.append(0)
    
    return EvaluationMetrics(
        precision=precision_score(page_labels, page_preds, zero_division=0),
        recall=recall_score(page_labels, page_preds, zero_division=0),
        f1=f1_score(page_labels, page_preds, zero_division=0),
        auc_roc=roc_auc_score(page_labels, page_probas),
        average_precision=average_precision_score(page_labels, page_probas),
        job_coverage_rate=np.mean(job_coverage_rates),
        title_accuracy=np.mean(title_accuracies),
        location_accuracy=0.0,  # compute similarly
        type_accuracy=0.0,
        domain_success_rate=np.mean(domain_successes),
        false_positive_rate=1 - precision_score(page_labels, page_preds, zero_division=0),
        pages_crawled_per_domain=0.0,
        time_per_domain_seconds=0.0,
    )
```

---

## 6. Pseudo-Labeling Strategy

This is the mechanism that makes the system self-improving. Be careful — label noise compounds.

```python
class PseudoLabelGenerator:
    
    # Only add pseudo-labels above this confidence
    MIN_CONFIDENCE = 0.85  
    
    # Cap how many pseudo-labels can come from a single crawl batch
    MAX_PSEUDO_RATIO = 0.4  # At most 40% of training data can be pseudo-labeled
    
    def generate(self, 
                 champion_pipeline,
                 unlabeled_pages: list[CrawledPage],
                 training_corpus: list[TrainingExample]) -> list[TrainingExample]:
        
        new_examples = []
        current_manual_count = sum(1 for e in training_corpus if e.label_source == 'manual')
        max_pseudo = int(current_manual_count * self.MAX_PSEUDO_RATIO / (1 - self.MAX_PSEUDO_RATIO))
        current_pseudo_count = sum(1 for e in training_corpus if e.label_source == 'pseudo')
        budget = max(0, max_pseudo - current_pseudo_count)
        
        # Score all unlabeled pages
        scored = []
        for page in unlabeled_pages:
            proba = champion_pipeline.classifier.predict_proba_single(page)
            scored.append((page, proba))
        
        # Sort by distance from 0.5 (most confident first)
        scored.sort(key=lambda x: abs(x[1] - 0.5), reverse=True)
        
        # Add high-confidence examples
        added = 0
        for page, proba in scored:
            if added >= budget:
                break
            
            if proba >= self.MIN_CONFIDENCE:
                new_examples.append(TrainingExample(
                    crawled_page_id=page.id,
                    label=1,
                    label_source='pseudo',
                    pseudo_confidence=proba,
                    is_holdout=False,
                ))
                added += 1
            elif proba <= (1 - self.MIN_CONFIDENCE):
                # High-confidence negatives are just as valuable
                new_examples.append(TrainingExample(
                    crawled_page_id=page.id,
                    label=0,
                    label_source='pseudo',
                    pseudo_confidence=1 - proba,
                    is_holdout=False,
                ))
                added += 1
        
        return new_examples
    
    def measure_pseudo_label_quality(self, 
                                      pseudo_labels: list[TrainingExample],
                                      later_verified: list[TrainingExample]) -> float:
        """After manual verification of a sample, compute agreement rate."""
        pseudo_dict = {e.crawled_page_id: e.label for e in pseudo_labels}
        verified_dict = {e.crawled_page_id: e.label for e in later_verified}
        
        shared_ids = set(pseudo_dict.keys()) & set(verified_dict.keys())
        if not shared_ids:
            return 1.0
        
        agreements = sum(1 for id in shared_ids 
                        if pseudo_dict[id] == verified_dict[id])
        return agreements / len(shared_ids)
```

---

## 7. Where LLMs Fit (Even in a Classical ML System)

Even though the core classifier is LightGBM, there are three places where an LLM dramatically improves the system:

### 7a. Failure Analysis — Prompting Claude to Understand Misses

After each experiment, run the champion and challenger on the holdout and collect failures. Feed them to Claude to get structured insights.

```python
FAILURE_ANALYSIS_PROMPT = """
You are helping improve a machine learning model that identifies career/jobs pages on company websites.

Below are {n} pages that the model INCORRECTLY classified. For each, you will see:
- The URL
- The page title  
- Key features (extracted from the page)
- Whether it was a false positive (classified as careers page but isn't) or false negative (is a careers page but was missed)
- The model's confidence score

Analyse these failures and identify:
1. PATTERNS — What do these failures have in common? (URL structure, content patterns, company type, ATS platform, etc.)
2. MISSING FEATURES — What signals exist in these pages that the model clearly isn't picking up on?
3. FEATURE IMPROVEMENTS — Suggest 3–5 specific, implementable new features (in Python regex/BeautifulSoup terms) that would likely fix these cases.
4. EDGE CASES — Are there structural categories of pages that this model will systematically struggle with?

Be specific and technical. Focus on actionable improvements to feature engineering.

---
FAILED CASES:
{cases}
---

Respond in JSON with keys: patterns, missing_features, suggested_features (list of {name, description, python_code}), edge_cases.
"""

def analyze_failures(failures: list[dict]) -> dict:
    cases_text = "\n\n".join([
        f"URL: {f['url']}\n"
        f"Title: {f['title']}\n"
        f"Type: {'False Positive' if f['is_false_positive'] else 'False Negative'}\n"
        f"Confidence: {f['confidence']:.3f}\n"
        f"Key features: {json.dumps({k: v for k, v in f['features'].items() if v != 0}, indent=2)}"
        for f in failures[:30]  # Cap at 30 to stay within context
    ])
    
    response = claude_client.messages.create(
        model="claude-opus-4-6",
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": FAILURE_ANALYSIS_PROMPT.format(
                n=len(failures[:30]),
                cases=cases_text
            )
        }]
    )
    
    text = response.content[0].text
    # Strip any markdown fencing before parsing
    clean = re.sub(r'```json\n?|```\n?', '', text).strip()
    return json.loads(clean)
```

### 7b. Synthetic Training Data Generation

Bootstrap your training set by generating synthetic examples. Especially useful for hard negatives (pages that LOOK like job pages but aren't).

```python
SYNTHETIC_DATA_PROMPT = """
Generate {n} realistic synthetic HTML snippets for training a career page classifier.

Generate {n_positive} POSITIVE examples (real career/jobs pages) and {n_negative} NEGATIVE examples.

For negatives, specifically generate hard negatives — pages that superficially resemble career pages but are NOT:
- HR policy pages (contains headings like "Our Benefits", "Leave Policy")
- Team/About pages (contains headings like "Meet the Team", "Our People")
- Blog posts about hiring (has job-related keywords but is editorial content)
- Competitor analysis pages (mentions job titles in context)
- Press releases about headcount

For each example output:
- url: a realistic URL
- title: realistic page title
- html_snippet: 200-400 words of realistic body HTML (use real company names, real job titles, realistic content)
- label: 1 for positive, 0 for negative
- hard_negative_type: (for negatives) which category above

Format as a JSON array.

Requirements:
- HTML must be realistic enough to fool a simple keyword matcher
- Include variety: different industries, company sizes, geographies
- Include some non-English examples (French, German, Spanish career pages)
"""

def generate_synthetic_training_data(n_positive=50, n_negative=100) -> list[dict]:
    response = claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8000,
        messages=[{
            "role": "user",
            "content": SYNTHETIC_DATA_PROMPT.format(
                n=n_positive + n_negative,
                n_positive=n_positive,
                n_negative=n_negative
            )
        }]
    )
    
    text = response.content[0].text
    clean = re.sub(r'```json\n?|```\n?', '', text).strip()
    return json.loads(clean)
```

### 7c. Challenger Config Generation

Use Claude to propose challenger configurations based on performance history.

```python
CHALLENGER_GENERATION_PROMPT = """
You are a machine learning engineer helping optimise a career page classifier.

Here is the history of experiments over the last {n} iterations:
{experiment_history}

The current champion has these characteristics:
- Config: {champion_config}
- Performance: {champion_metrics}
- Top 10 most important features: {feature_importance}
- Most common failure modes: {failure_modes}

Based on this history, propose ONE specific challenger configuration that is likely to beat the champion.

Consider:
- Which features are underweighted (low importance but logically should matter)?
- Which hyperparameter dimensions haven't been explored?
- Are there any ensemble strategies not yet tried?
- What pseudo-label threshold seems to be working best?
- Are there new features suggested by the failure analysis that haven't been added yet?

Return a JSON object with keys:
- strategy: which ChallengerStrategy enum value to use
- rationale: 2-3 sentences explaining why this challenger is likely to win
- config_changes: dict of specific changes to make to the champion config
- expected_improvement: which metric you expect to improve and by roughly how much
"""

def propose_challenger(experiment_history: list, champion: dict) -> dict:
    history_text = "\n".join([
        f"Iteration {i+1}: {e['challenger_strategy']} | "
        f"challenger_f1={e['challenger_metrics']['f1']:.4f} | "
        f"champion_f1={e['champion_metrics']['f1']:.4f} | "
        f"outcome={e['outcome']}"
        for i, e in enumerate(experiment_history[-10:])  # Last 10
    ])
    
    response = claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[{
            "role": "user",
            "content": CHALLENGER_GENERATION_PROMPT.format(
                n=len(experiment_history),
                experiment_history=history_text,
                champion_config=json.dumps(champion['config'], indent=2),
                champion_metrics=json.dumps(champion['metrics'], indent=2),
                feature_importance=json.dumps(champion['feature_importance'][:10], indent=2),
                failure_modes=json.dumps(champion.get('failure_modes', []), indent=2),
            )
        }]
    )
    
    text = response.content[0].text
    clean = re.sub(r'```json\n?|```\n?', '', text).strip()
    return json.loads(clean)
```

### 7d. ATS Pattern Discovery

Use Claude to generate CSS selectors for new/unknown ATS platforms you encounter in crawls.

```python
ATS_DISCOVERY_PROMPT = """
You are helping build a job listing extractor. A crawler has found a page that is clearly a jobs/careers page, but it doesn't match any known ATS platform patterns.

Here is the raw HTML of the page (truncated to 5000 characters):
{html}

The page URL is: {url}

1. Identify what ATS/hiring platform this page is using (if any). Look for:
   - Platform-specific CSS classes or IDs
   - Known ATS API endpoints in JavaScript
   - Known ATS embed scripts
   - Unique structural patterns

2. Write CSS selectors to extract:
   - The container for each individual job listing
   - The job title within a listing
   - The location within a listing
   - The department/team within a listing
   - The employment type within a listing
   - The apply link within a listing

3. Note any pagination patterns (infinite scroll, load more button, page number URLs).

Return JSON with keys:
- ats_platform: name or null
- confidence: 0-1 confidence that you correctly identified the platform
- selectors: {job_list, title, location, department, type, apply_url}
- pagination: {type: "none"|"button"|"url", selector: ..., url_pattern: ...}
- notes: any caveats or special handling needed
"""
```

---

## 8. Model Versioning and Experiment Tracking

### What to Log in MLflow

```python
import mlflow

def log_experiment(config: PipelineConfig, metrics: EvaluationMetrics, 
                   model, feature_importance: pd.DataFrame):
    
    with mlflow.start_run():
        # Config as params
        mlflow.log_params({
            'classifier_type': config.classifier_type,
            'embedding_model': config.embedding_model,
            'pseudo_label_min_confidence': config.pseudo_label_min_confidence,
            'use_url_features': config.use_url_features,
            'use_embeddings': config.use_embeddings,
            'use_graph_features': config.use_graph_features,
            **(config.classifier_params or {}),
        })
        
        # Metrics
        mlflow.log_metrics({
            'f1': metrics.f1,
            'precision': metrics.precision,
            'recall': metrics.recall,
            'auc_roc': metrics.auc_roc,
            'average_precision': metrics.average_precision,
            'job_coverage_rate': metrics.job_coverage_rate,
            'title_accuracy': metrics.title_accuracy,
            'domain_success_rate': metrics.domain_success_rate,
            'primary_score': metrics.primary_score,
        })
        
        # Feature importance as artifact
        mlflow.log_table(feature_importance, 'feature_importance.json')
        
        # Model artifact
        mlflow.lightgbm.log_model(model, 'model')
        
        return mlflow.active_run().info.run_id
```

---

## 9. Anti-Patterns to Avoid

**Don't use accuracy as your metric.** With 1-3% positive rate, a model that predicts all-negative gets 97%+ accuracy. Use F1, Average Precision, or AUC-ROC.

**Don't train and evaluate on the same distribution.** Your training corpus grows via pseudo-labels from the champion model. This means it increasingly reflects the champion's view of the world. Your holdout baseline is your only truly independent signal — protect it fiercely.

**Don't let pseudo-label noise compound across too many generations.** Consider "generational decay": pseudo-labels more than N generations old should be down-weighted or purged. The training corpus should have an approximate freshness distribution.

**Don't crawl without a URL budget per domain.** Without a cap, a site with thousands of pages (e.g. an enterprise company site) will drain your crawl budget. Set a hard limit of 200 pages per domain and prioritize aggressively.

**Don't treat all ATS platforms equally.** Workday and SuccessFactors are JS-heavy enterprise systems — they require Playwright and have complex pagination. Greenhouse and Lever are mostly static and easy. Taleo is notoriously inconsistent. Maintain per-ATS reliability scores.

**Don't retrain from scratch every experiment.** For faster iteration, use LightGBM's `init_model` parameter to warm-start a challenger from the champion's tree weights. This converges faster and makes incremental improvements cheaper to evaluate.

```python
# Warm-start a challenger from the champion
challenger_model = lgb.train(
    challenger_params,
    dtrain,
    num_boost_round=100,
    init_model=champion_model,  # warm start
    valid_sets=[dval],
    callbacks=[lgb.early_stopping(20)]
)
```

---

## 10. Recommended Libraries Summary

| Purpose | Library | Notes |
|---|---|---|
| Core ML classifier | `lightgbm` | Your primary model |
| Feature extraction | `beautifulsoup4 + lxml` | Fast HTML parsing |
| Embeddings | `sentence-transformers` | all-MiniLM-L6-v2 for speed |
| Dim reduction | `scikit-learn` PCA | Reduce embedding dims before LGB |
| CRF extraction | `sklearn-crfsuite` | Field labeling |
| Hyperparam opt | `optuna` | Bayesian search, integrates with LGB |
| Fuzzy matching | `rapidfuzz` | For extraction accuracy scoring |
| Crawling (static) | `httpx` + `selectolax` | selectolax is 10x faster than BS4 |
| Crawling (JS) | `playwright` | For Workday, React-heavy sites |
| Experiment tracking | `mlflow` | Model registry + metric history |
| DOM clustering | `scikit-learn` DBSCAN | For DOM pattern extractor |
| LLM calls | `anthropic` SDK | Failure analysis, challenger gen |
