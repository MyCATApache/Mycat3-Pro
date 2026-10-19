#!/usr/bin/env bash
# 发布 Mycat3 Pro 镜像到 Docker Hub，并同步 Docker Hub 仓库简介。
#
# 用法:
#   DOCKERHUB_USER=<账号> DOCKERHUB_TOKEN=<PAT> ./scripts/publish-dockerhub.sh
#   ./scripts/publish-dockerhub.sh --dry-run
#   ./scripts/publish-dockerhub.sh --description-only
#   PLATFORMS=linux/amd64,linux/arm64 ./scripts/publish-dockerhub.sh
#
# 说明:
# - DOCKERHUB_TOKEN 使用 Docker Hub Personal Access Token（需 Read & Write），
#   不要使用账号密码；账号必须拥有镜像命名空间（默认 mycat）的写权限。
# - 未提供 DOCKERHUB_* 时，推送沿用本机已有 docker login；此时无法通过脚本
#   更新 Hub 描述（脚本不读取 Docker 凭据文件），会打印手动操作提示。
# - 默认推送已构建的本地镜像（v<version> 先推、latest 后推）；设置 PLATFORMS
#   则用 buildx 重新构建多架构并推送（不会使用本地镜像层）。
# - --description-only 只更新 Docker Hub 仓库简介，需要 DOCKERHUB_*。
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE="${IMAGE:-mycat/mycat3-pro}"
VERSION="${VERSION:-}"
PLATFORMS="${PLATFORMS:-}"
HUB_README="${HUB_README:-$ROOT/docker/README.md}"
SHORT_DESC="${SHORT_DESC:-Mycat3 Pro: MySQL/PostgreSQL/TDS middleware with sharding and proxy modes.}"
DRY_RUN=0
DESCRIPTION_ONLY=0
SKIP_DESCRIPTION="${SKIP_DESCRIPTION:-0}"

die() { echo "错误: $*" >&2; exit 1; }
info() { echo "==> $*"; }
warn() { echo "警告: $*" >&2; }

usage() {
    awk 'NR > 1 && /^set -euo pipefail/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --description-only) DESCRIPTION_ONLY=1 ;;
        --skip-description) SKIP_DESCRIPTION=1 ;;
        -h|--help) usage; exit 0 ;;
        *) die "未知参数: $1（--help 查看用法）" ;;
    esac
    shift
done

if [ -z "$VERSION" ]; then
    VERSION="$(python3 - "$ROOT/Cargo.toml" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
section = re.search(r"^\[workspace\.package\](.*?)(?=^\[|\Z)", text, re.M | re.S)
scope = section.group(1) if section else text
match = re.search(r'^version\s*=\s*"([^"]+)"', scope, re.M)
print(match.group(1) if match else "0.0.0")
PY
)"
fi
VERSION="${VERSION#v}"
[ -n "$VERSION" ] || die "无法确定版本号，请显式设置 VERSION"

TAGS=("v${VERSION}" "latest")
if [ "$VERSION" = "latest" ]; then
    TAGS=("latest")
fi

command -v curl >/dev/null 2>&1 || die "需要 curl"
command -v python3 >/dev/null 2>&1 || die "需要 python3"
[ -f "$HUB_README" ] || die "缺少 Docker Hub 简介文件: $HUB_README"

short_len="$(python3 -c 'import sys; print(len(sys.argv[1]))' "$SHORT_DESC")"
[ "$short_len" -le 100 ] || die "SHORT_DESC 超过 Docker Hub 100 字符限制（当前 $short_len）"

if [ "$DESCRIPTION_ONLY" -eq 0 ]; then
    command -v docker >/dev/null 2>&1 || die "需要 docker"
    if [ -z "$PLATFORMS" ]; then
        for tag in "${TAGS[@]}"; do
            docker image inspect "$IMAGE:$tag" >/dev/null 2>&1 \
                || die "本地缺少镜像 $IMAGE:$tag，请先构建（见 docs/docker.md）"
        done
        baked_version="$(docker image inspect "$IMAGE:latest" \
            --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' 2>/dev/null || true)"
        if [ -n "$baked_version" ] && [ "$baked_version" != "$VERSION" ]; then
            warn "本地 latest 镜像的 OCI version 标签是 ${baked_version}，与脚本 VERSION=${VERSION} 不一致"
        fi
    else
        docker buildx version >/dev/null 2>&1 \
            || die "PLATFORMS 需要 docker buildx（安装 buildx 并启用 binfmt/QEMU）"
    fi
fi

if [ "$DRY_RUN" -eq 1 ]; then
    info "dry-run：不会登录、推送或修改 Docker Hub"
    info "镜像: $IMAGE"
    info "版本: v$VERSION"
    info "标签: ${TAGS[*]}"
    if [ "$DESCRIPTION_ONLY" -eq 0 ]; then
        if [ -n "$PLATFORMS" ]; then
            info "构建平台: $PLATFORMS（buildx --push）"
        else
            info "推送方式: 本地已有镜像（单架构）"
        fi
        info "凭据: DOCKERHUB_USER=${DOCKERHUB_USER:-<未设置>} DOCKERHUB_TOKEN=${DOCKERHUB_TOKEN:+<已设置>}${DOCKERHUB_TOKEN:-<未设置>}"
    fi
    info "Docker Hub 简介: $HUB_README（${short_len} 字符短描述）"
    [ "$SKIP_DESCRIPTION" -eq 1 ] && info "描述更新: 跳过（--skip-description）"
    exit 0
fi

HUB_USER="${DOCKERHUB_USER:-}"
HUB_TOKEN="${DOCKERHUB_TOKEN:-}"
DOCKER_CONFIG_DIR="${DOCKER_CONFIG:-$HOME/.docker}"

if [ "$DESCRIPTION_ONLY" -eq 1 ] && { [ -z "$HUB_TOKEN" ] || [ -z "$HUB_USER" ]; }; then
    die "--description-only 需要同时设置 DOCKERHUB_USER 和 DOCKERHUB_TOKEN"
fi

if [ "$DESCRIPTION_ONLY" -eq 0 ]; then
    if [ -n "$HUB_TOKEN" ]; then
        [ -n "$HUB_USER" ] || die "设置了 DOCKERHUB_TOKEN 但缺少 DOCKERHUB_USER"
        info "登录 Docker Hub（账号: $HUB_USER）"
        printf '%s' "$HUB_TOKEN" | docker login -u "$HUB_USER" --password-stdin
    else
        [ -f "$DOCKER_CONFIG_DIR/config.json" ] \
            || die "未提供 DOCKERHUB_USER/DOCKERHUB_TOKEN，且本机没有 docker login（$DOCKER_CONFIG_DIR/config.json）"
        warn "未提供 DOCKERHUB_*，沿用本机已有 docker login；将跳过 Hub 描述更新"
    fi

    if [ -n "$PLATFORMS" ]; then
        info "构建并推送多架构镜像: $PLATFORMS"
        docker buildx build \
            --platform "$PLATFORMS" \
            --build-arg "VERSION=$VERSION" \
            -t "$IMAGE:v$VERSION" -t "$IMAGE:latest" \
            --push "$ROOT"
    else
        for tag in "${TAGS[@]}"; do
            info "推送 $IMAGE:$tag"
            docker push "$IMAGE:$tag"
        done
    fi
    info "镜像推送完成: $IMAGE:latest, $IMAGE:v$VERSION"
fi

if [ "$SKIP_DESCRIPTION" -eq 1 ]; then
    info "已跳过 Docker Hub 描述更新"
    exit 0
fi

if [ -z "$HUB_TOKEN" ] || [ -z "$HUB_USER" ]; then
    warn "未提供 DOCKERHUB_USER/DOCKERHUB_TOKEN，跳过 Docker Hub 描述更新"
    echo "   可在 Docker Hub 仓库设置中粘贴 docker/README.md，或运行："
    echo "   DOCKERHUB_USER=... DOCKERHUB_TOKEN=... $0 --description-only"
    exit 0
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# Hub API 登录（token 不进入命令行参数；响应文件仅当前用户可读）
HUB_USER="$HUB_USER" HUB_TOKEN="$HUB_TOKEN" python3 >"$tmpdir/login.json" <<'PY'
import json
import os
import sys

json.dump(
    {"username": os.environ["HUB_USER"], "password": os.environ["HUB_TOKEN"]},
    sys.stdout,
)
PY

login_code="$(curl -sS --max-time 30 -o "$tmpdir/login-resp.json" -w '%{http_code}' \
    -X POST https://hub.docker.com/v2/users/login/ \
    -H 'Content-Type: application/json' --data @"$tmpdir/login.json" || true)"
if [ "$login_code" != "200" ]; then
    warn "Docker Hub API 登录失败（HTTP ${login_code:-000}），描述未更新"
    head -c 300 "$tmpdir/login-resp.json" 2>/dev/null || true
    echo
    echo "   请确认 PAT 具有 Read & Write 权限、网络可访问 hub.docker.com，"
    echo "   或手动把 docker/README.md 粘贴到 Docker Hub 仓库简介。"
    exit 1
fi

HUB_JWT="$(python3 - "$tmpdir/login-resp.json" <<'PY'
import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8")).get("token", ""))
PY
)"
[ -n "$HUB_JWT" ] || die "Docker Hub API 登录响应中没有 token"

HUB_README="$HUB_README" SHORT_DESC="$SHORT_DESC" python3 >"$tmpdir/repo.json" <<'PY'
import json
import os
import sys

with open(os.environ["HUB_README"], encoding="utf-8") as f:
    readme = f.read()
json.dump(
    {
        "description": os.environ["SHORT_DESC"],
        "full_description": readme,
    },
    sys.stdout,
    ensure_ascii=False,
)
PY

# Authorization 通过 curl 配置文件传递，避免出现在进程参数中
printf 'header = "Authorization: JWT %s"\n' "$HUB_JWT" >"$tmpdir/curl-auth.conf"

info "更新 Docker Hub 仓库简介: $IMAGE"
patch_code="$(curl -sS --max-time 60 -K "$tmpdir/curl-auth.conf" -o "$tmpdir/patch-resp.json" \
    -w '%{http_code}' -X PATCH "https://hub.docker.com/v2/repositories/$IMAGE/" \
    -H 'Content-Type: application/json' --data @"$tmpdir/repo.json" || true)"
if [ "$patch_code" != "200" ]; then
    warn "Docker Hub 描述更新失败（HTTP ${patch_code:-000}）"
    head -c 500 "$tmpdir/patch-resp.json" 2>/dev/null || true
    echo
    echo "   首次推送会自动创建仓库；如果仓库属于其他账号/组织，请确认账号对命名空间有写权限。"
    echo "   也可手动把 docker/README.md 粘贴到 Docker Hub 仓库简介。"
    exit 1
fi

info "Docker Hub 仓库简介已更新"
echo "   https://hub.docker.com/r/$IMAGE"
