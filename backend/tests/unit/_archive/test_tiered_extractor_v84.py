from app.crawlers.tiered_extractor_v84 import TieredExtractorV84


def test_extract_jobvite_table_rows_v84_keeps_large_sets():
    extractor = TieredExtractorV84()
    rows = []
    for i in range(1, 26):
        rows.append(
            f"""
            <tr>
              <td class="jv-job-list-name"><a href="/acme/job/o{i:04d}">Software Engineer {i}</a></td>
              <td class="jv-job-list-comp">Technology</td>
              <td class="jv-job-list-location">Sydney</td>
            </tr>
            """
        )
    html = f"<table class='jv-job-list'><tbody>{''.join(rows)}</tbody></table>"

    jobs = extractor._extract_jobvite_table_rows_v84(html, "https://jobs.jobvite.com/acme/jobs")

    assert len(jobs) == 25
    assert jobs[0]["title"] == "Software Engineer 1"
    assert jobs[-1]["title"] == "Software Engineer 25"
    assert all("/job/" in j["source_url"] for j in jobs)


def test_extract_talentsoft_offer_rows_v84():
    extractor = TieredExtractorV84()
    html = """
    <ul>
      <li class="ts-offer-list-item offerlist-item">
        <h3 class="ts-offer-list-item__title styleh3">
          <a class="ts-offer-list-item__title-link" href="/offre-de-emploi/emploi-controleur-qualite-immobilier-f-h_4463.aspx">Contrôleur qualité immobilier F/H</a>
        </h3>
        <ul class="ts-offer-list-item__description"><li>CDG</li><li>CDI</li></ul>
      </li>
      <li class="ts-offer-list-item offerlist-item">
        <h3 class="ts-offer-list-item__title styleh3">
          <a class="ts-offer-list-item__title-link" href="/offre-de-emploi/emploi-technicien-maintenance-travaux-f-h_4593.aspx">Technicien maintenance travaux F/H</a>
        </h3>
        <ul class="ts-offer-list-item__description"><li>Orly</li><li>CDI</li></ul>
      </li>
      <li class="ts-offer-list-item offerlist-item">
        <h3 class="ts-offer-list-item__title styleh3">
          <a class="ts-offer-list-item__title-link" href="/offre-de-emploi/emploi-fiscaliste-senior-e-groupe-adp-f-h_4679.aspx">Fiscaliste senior groupe ADP F/H</a>
        </h3>
        <ul class="ts-offer-list-item__description"><li>Le Bourget</li><li>CDI</li></ul>
      </li>
    </ul>
    """

    jobs = extractor._extract_talentsoft_offer_rows_v84(
        html,
        "https://groupeadp-recrute.talent-soft.com/offre-de-emploi/liste-offres.aspx",
    )

    assert len(jobs) == 3
    assert jobs[0]["title"] == "Contrôleur qualité immobilier F/H"
    assert jobs[0]["location_raw"] == "CDG"
    assert jobs[0]["source_url"].endswith("_4463.aspx")


def test_extract_jobs2web_dom_v84_supports_jobslist_cards():
    extractor = TieredExtractorV84()
    html = """
    <ul class="JobsList_jobCardResultList__At-4d">
      <li class="JobsList_jobCard__8wE-Z">
        <a class="jobCardTitle JobsList_jobCardTitle__pRNjw" href="/job/marketing-knowledge/1234">Marketing & Knowledge Mgmt Coordinator</a>
        <div><span class="JobsList_jobCardFooterValue__Lc--j">Hong Kong</span></div>
      </li>
      <li class="JobsList_jobCard__8wE-Z">
        <a class="jobCardTitle JobsList_jobCardTitle__pRNjw" href="/job/senior-site-engineer/2345">Senior Site Engineer (E&M)</a>
        <div><span class="JobsList_jobCardFooterValue__Lc--j">Singapore</span></div>
      </li>
      <li class="JobsList_jobCard__8wE-Z">
        <a class="jobCardTitle JobsList_jobCardTitle__pRNjw" href="/job/senior-project-coordinator/3456">Senior Project Coordinator</a>
        <div><span class="JobsList_jobCardFooterValue__Lc--j">Kuala Lumpur</span></div>
      </li>
    </ul>
    """

    jobs = extractor._extract_jobs2web_dom_v66(
        html,
        "https://careers.example.com/search/?q=&locationsearch=&searchResultView=LIST",
    )

    assert len(jobs) == 3
    assert jobs[0]["title"] == "Marketing & Knowledge Mgmt Coordinator"
    assert jobs[0]["location_raw"] == "Hong Kong"


def test_jobs2web_endpoint_candidates_v84_prioritize_current_search_url():
    extractor = TieredExtractorV84()
    cfg = {
        "company_id": "Bausch",
        "locale": "en_US",
        "api_url": "https://api2.successfactors.eu",
        "csrf": "token",
        "referrer": "rmk-map-2.jobs2web.com",
    }
    page_url = (
        "https://careers.bauschhealth.com/search?searchResultView=LIST&pageNumber=0"
        "&markerViewed=&carouselIndex=&facetFilters=%7B%7D&sortBy=date"
    )

    endpoints = extractor._jobs2web_endpoint_candidates_v66(page_url, cfg)

    assert endpoints
    assert "searchresultview=list" in endpoints[0].lower()
    assert "careers.bauschhealth.com/search" in endpoints[0].lower()
