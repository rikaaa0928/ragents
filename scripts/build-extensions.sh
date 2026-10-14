#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

# 支持通过参数传入以逗号分隔的扩展名列表，如: ./scripts/build-extensions.sh shell,file_editor,image_viewer
# 默认编译所有扩展
if [ "$#" -gt 1 ]; then
    echo "usage: $0 [shell,file_editor,image_viewer]" >&2
    exit 2
fi

filter=$(printf '%s' "${1:-}" | tr -d '[:space:]')
if [ "$#" -eq 1 ] && [ -z "$filter" ]; then
    echo "Error: extension filter must not be empty" >&2
    exit 2
fi

ALL_EXTENSIONS="shell,file_editor,image_viewer"

is_selected() {
    ext_name="$1"
    if [ -z "$filter" ]; then
        return 0
    fi
    # 检查 $filter (以逗号分隔) 中是否包含该扩展名
    case ",$filter," in
        *,"$ext_name",*) return 0 ;;
        *) return 1 ;;
    esac
}

# 校验传入的扩展名是否存在
if [ -n "$filter" ]; then
    old_ifs="$IFS"
    IFS=','
    for ext in $filter; do
        case ",$ALL_EXTENSIONS," in
            *,"$ext",*) ;;
            *)
                echo "Error: unknown extension '$ext'. Available extensions: $ALL_EXTENSIONS" >&2
                exit 1
                ;;
        esac
    done
    IFS="$old_ifs"
fi

build_extension() {
    manifest_path="$1"
    component_path="$2"
    cargo build \
        --manifest-path "$manifest_path" \
        --target wasm32-wasip2 \
        --release
    echo "$component_path"
}

if is_selected "shell"; then
    build_extension \
        "$project_dir/extensions/shell/Cargo.toml" \
        "$project_dir/extensions/shell/target/wasm32-wasip2/release/ragent_shell_extension.wasm"
fi

if is_selected "file_editor"; then
    build_extension \
        "$project_dir/extensions/file_editor/Cargo.toml" \
        "$project_dir/extensions/file_editor/target/wasm32-wasip2/release/ragent_file_editor_extension.wasm"
fi

if is_selected "image_viewer"; then
    build_extension \
        "$project_dir/extensions/image_viewer/Cargo.toml" \
        "$project_dir/extensions/image_viewer/target/wasm32-wasip2/release/ragent_image_viewer_extension.wasm"
fi
