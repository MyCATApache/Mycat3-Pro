#!/usr/bin/env bash
# 本地/离线交叉编译发布包（GitHub Release 的 CI 使用同一目标矩阵；
# 本脚本用于无 CI 或内网环境）。
#
# 用法:
#   scripts/build-release.sh [VERSION] [TARGET...]
#
# 默认版本取 Cargo.toml workspace.package.version。
# 默认目标（在 Linux 上构建 Windows 包时使用 *-windows-gnu，避免 MSVC 链接器）:
#   x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
#   x86_64-pc-windows-gnu   aarch64-pc-windows-gnu
#
# 依赖（按目标）:
#   aarch64-unknown-linux-gnu : gcc-aarch64-linux-gnu（apt）或等价交叉 gcc
#   *-pc-windows-gnu          : mingw-w64（x86_64）或 llvm-mingw（aarch64）
#
# 产物: dist/mycat3-pro-<version>-<target>.{tar.gz,zip} + SHA256SUMS.txt

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VERSION="${1:-$(awk '/^\[workspace.package\]/{f=1} f&&/^version/{gsub(/[",]/," ");print $3;exit}' Cargo.toml)}"
if [ "$#" -gt 0 ]; then shift; fi
if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  TARGETS=(
    x86_64-unknown-linux-gnu
    aarch64-unknown-linux-gnu
    x86_64-pc-windows-gnu
    aarch64-pc-windows-gnu
  )
fi

DIST="$ROOT/dist"
mkdir -p "$DIST"

command -v cargo >/dev/null || { echo "cargo 未安装" >&2; exit 1; }
command -v rustup >/dev/null || { echo "rustup 未安装（需要 rustup target add）" >&2; exit 1; }

echo "== Mycat3 Pro release $VERSION =="
echo "targets: ${TARGETS[*]}"

for target in "${TARGETS[@]}"; do
  echo
  echo "--> $target"
  rustup target add "$target" >/dev/null 2>&1 || true

  case "$target" in
    aarch64-unknown-linux-gnu)
      if command -v aarch64-linux-gnu-gcc >/dev/null; then
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
      else
        echo "缺少 aarch64-linux-gnu-gcc（apt install gcc-aarch64-linux-gnu）" >&2
        exit 1
      fi
      ;;
    x86_64-pc-windows-gnu)
      command -v x86_64-w64-mingw32-gcc >/dev/null || {
        echo "缺少 x86_64-w64-mingw32-gcc（apt install mingw-w64）" >&2; exit 1;
      }
      export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
      ;;
    aarch64-pc-windows-gnu)
      if command -v aarch64-w64-mingw32-gcc >/dev/null; then
        export CARGO_TARGET_AARCH64_PC_WINDOWS_GNU_LINKER=aarch64-w64-mingw32-gcc
      elif command -v llvm-mingw/bin/aarch64-w64-mingw32-gcc >/dev/null; then
        export CARGO_TARGET_AARCH64_PC_WINDOWS_GNU_LINKER=llvm-mingw/bin/aarch64-w64-mingw32-gcc
      else
        echo "缺少 aarch64 mingw 链接器（llvm-mingw）" >&2
        exit 1
      fi
      ;;
  esac

  cargo build --release --locked --target "$target" -p mycat3-server

  stage="$DIST/.stage-$target"
  rm -rf "$stage"
  mkdir -p "$stage"
  cp README.md LICENSE "$stage"/
  cp -r conf "$stage"/conf

  case "$target" in
    *windows*)
      cp "target/$target/release/mycat3-pro.exe" "$stage"/
      (cd "$stage" && zip -qr "$DIST/mycat3-pro-$VERSION-$target.zip" .)
      ;;
    *)
      cp "target/$target/release/mycat3-pro" "$stage"/
      tar -czf "$DIST/mycat3-pro-$VERSION-$target.tar.gz" -C "$stage" .
      ;;
  esac
  rm -rf "$stage"
  echo "    打包完成: mycat3-pro-$VERSION-$target"
done

( cd "$DIST" && sha256sum mycat3-pro-* > SHA256SUMS.txt )
echo
echo "全部产物在 $DIST:"
ls -lh "$DIST" | tail -n +2
