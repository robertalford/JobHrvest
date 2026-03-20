#!/usr/bin/env python3
"""
Ensure auth credentials are configured on deploy.

If APP_PASSWORD_HASH is empty in the environment, this script generates a
bcrypt hash from SEED_PASSWORD (or the compiled-in default) and writes
APP_USERNAME + APP_PASSWORD_HASH into the local .env file so subsequent
uvicorn workers pick them up.

Run once at startup: python scripts/ensure_auth.py
"""
import os
import sys

ENV_FILE = os.path.join(os.path.dirname(__file__), '..', '.env')

# Default seed credentials — used only if .env has no password hash.
DEFAULT_USERNAME = 'r.m.l.alford@gmail.com'
DEFAULT_PASSWORD = os.environ.get('SEED_PASSWORD', 'Uu00dyandben!')


def _env_pairs(path: str) -> list[tuple[str, str]]:
    pairs = []
    try:
        with open(path) as f:
            for line in f:
                line = line.rstrip('\n')
                if '=' in line and not line.lstrip().startswith('#'):
                    k, _, v = line.partition('=')
                    pairs.append((k.strip(), v.strip()))
                else:
                    pairs.append(('__comment__', line))
    except FileNotFoundError:
        pass
    return pairs


def _write_env(path: str, pairs: list[tuple[str, str]]) -> None:
    with open(path, 'w') as f:
        for k, v in pairs:
            if k == '__comment__':
                f.write(v + '\n')
            else:
                f.write(f'{k}={v}\n')


def main() -> None:
    username = os.environ.get('APP_USERNAME', '').strip()
    password_hash = os.environ.get('APP_PASSWORD_HASH', '').strip()

    if username and password_hash:
        print('[ensure_auth] APP_USERNAME and APP_PASSWORD_HASH are set — no action needed.')
        return

    import bcrypt
    print('[ensure_auth] APP_PASSWORD_HASH is empty — seeding default credentials.')
    hashed = bcrypt.hashpw(DEFAULT_PASSWORD.encode(), bcrypt.gensalt(rounds=12)).decode()

    pairs = _env_pairs(ENV_FILE)
    keys_present = {k for k, _ in pairs if k != '__comment__'}

    updated: list[tuple[str, str]] = []
    username_set = False
    hash_set = False
    for k, v in pairs:
        if k == 'APP_USERNAME':
            updated.append(('APP_USERNAME', DEFAULT_USERNAME))
            username_set = True
        elif k == 'APP_PASSWORD_HASH':
            # Escape $ as 82031 for docker-compose variable substitution
            updated.append(('APP_PASSWORD_HASH', hashed.replace('$', '82031')))
            hash_set = True
        else:
            updated.append((k, v))

    if not username_set:
        updated.append(('APP_USERNAME', DEFAULT_USERNAME))
    if not hash_set:
        updated.append(('APP_PASSWORD_HASH', hashed.replace('$', '82031')))

    _write_env(ENV_FILE, updated)
    print(f'[ensure_auth] Wrote APP_USERNAME={DEFAULT_USERNAME} and APP_PASSWORD_HASH to {ENV_FILE}')
    print('[ensure_auth] NOTE: Restart the service for the new credentials to take effect.')


if __name__ == '__main__':
    main()
