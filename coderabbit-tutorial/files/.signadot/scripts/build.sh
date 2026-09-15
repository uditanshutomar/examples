#!/usr/bin/env bash
set -euo pipefail
case "${TARGET_ARCH:?}" in amd64|arm64) ;; *) exit 1 ;; esac
npm install --global yarn@1.22.22
(
  cd services/frontend/react_app
  yarn install --frozen-lockfile
  yarn build
)
# Match HotROD's frontend build script; make its asset URLs work under /web_assets.
python3 - <<'PY'
from pathlib import Path
p = Path('services/frontend/web_assets/index.html')
p.write_text(p.read_text().replace('src="/assets', 'src="/web_assets/assets')
             .replace('href="/assets', 'href="/web_assets/assets'))
PY
go test ./...
go vet ./...
mkdir -p "dist/linux/$TARGET_ARCH/bin"
CGO_ENABLED=0 GOOS=linux GOARCH="$TARGET_ARCH" \
  go build -trimpath -o "dist/linux/$TARGET_ARCH/bin/hotrod" ./cmd/hotrod
