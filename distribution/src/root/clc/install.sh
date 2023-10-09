#! /bin/bash

# Hazelcast CLC Install script
# (c) 2023 Hazelcast, Inc.

set -eu -o pipefail

check_ok () {
    local what="$1"
    local e=no
    which "$what" > /dev/null && e=yes
    case "$what" in
        awk*) state_awk_ok=$e;;
        bash*) state_bash_ok=$e;;
        curl*) state_curl_ok=$e;;
        tar*) state_tar_ok=$e;;
        unzip*) state_unzip_ok=$e;;
        wget*) state_wget_ok=$e;;
        xattr*) state_xattr_ok=$e;;
        zsh*) state_zsh_ok=$e;;
        *) log_debug "invalid check: $what"
    esac
}

log_warn () {
    echo "WARN  $1" 1>&2
}

log_info () {
    echo "INFO  $1" 1>&2
}

log_debug () {
    if [[ "${state_debug}" == "yes" ]]; then
        echo "DEBUG $1" 1>&2
    fi
}

echo_indent () {
    printf "      %s\n" "$1" 1>&2
}

echo_note () {
    echo "NOTE  $1" 1>&2
}

echo_ok () {
    echo "  OK  $1" 1>&2
}

bye () {
    if [[ "${1:-}" != "" ]]; then
        echo "ERROR $*" 1>&2
    fi
    exit 1
}

print_usage () {
    echo "This script installs Hazelcast CLC to a user directory."
    echo
    echo "Usage: $0 [--beta | --debug | --help]"
    echo
    echo "    --beta   Enable downloading BETA and PREVIEW releases"
    echo "    --debug  Enable DEBUG logging"
    echo "    --help   Show help"
    echo
    exit 0
}

setup () {
    detect_tmpdir
    for cmd in $DEPENDENCIES; do
        check_ok "$cmd"
    done
    detect_httpget
}

detect_tmpdir () {
    state_tmp_dir="${TMPDIR:-/tmp}"
}

do_curl () {
    curl -LSs "$1"
}

do_wget () {
    wget -O- "$1"
}

detect_uncompress () {
    local ext=${state_archive_ext}
    if [[ "$ext" == "tar.gz" ]]; then
        state_uncompress=do_untar
    elif [[ "$ext" == "zip" ]]; then
        state_uncompress=do_unzip
    else
        bye "$ext archive is not supported"
    fi
}

do_untar () {
    if [[ "$state_tar_ok" != "yes" ]]; then
        bye "tar is required for install"
    fi
    local path="$1"
    local base="$2"
    tar xf "$path" -C "$base"
}

do_unzip () {
    if [[ "$state_unzip_ok" != "yes" ]]; then
        bye "unzip is required for install"
    fi
    local path="$1"
    local base="$2"
    unzip -o -q "$path" -d "$base"
}

install_release () {
    # create base
    local tmp="${state_tmp_dir}"
    local base="$tmp/clc"
    mkdir -p "$base"
    # uncompress release package
    local path="${state_archive_path}"
    log_debug "UNCOMPRESS $path => $base"
    ${state_uncompress} "$path" "$base"
    # move files to their place
    base="$base/${state_clc_name}"
    local bin="$state_bin_dir/clc"
    mv_path "$base/clc" "$bin"
    local files="README.txt LICENSE.txt"
    for item in $files; do
        mv_path "$base/$item" "$CLC_HOME/$item"
    done
    # on MacOS remove the clc binary from quarantine
    if [[ "$state_xattr_ok" == "yes" && "$state_os" == "darwin" ]]; then
      set +e
      remove_from_quarantine "$bin"
      set -e
    fi
}

remove_from_quarantine () {
    local qa
    local path
    qa="com.apple.quarantine"
    path="$1"
    for a in $(xattr "$path"); do
    if [[ "$a" == "$qa" ]]; then
        log_debug "REMOVE FROM QUARANTINE: $path"
        xattr -d $qa "$path"
        break
    fi
    done
}

update_config_files () {
    if [[ "$state_bash_ok" == "yes" ]]; then
        update_rc "$HOME/.bashrc"
        update_rc "$HOME/.profile"
    fi
    if [[ "$state_zsh_ok" == "yes" ]]; then
        update_rc "$HOME/.zshenv"
    fi
}

update_rc () {
    local path="$1"
    local set_path="PATH=\$PATH:${state_bin_dir}"
    local code="
echo \"\$PATH\" | grep \"${state_bin_dir}\" > /dev/null
if [[ \$? == 1 ]]; then
 export $set_path
fi
"
    if [[ -e "$path" ]]; then
        # check if this file is a symbolic link
        if [[ -L "$path" ]]; then
            log_warn "$path is a symbolic link. Writing to symbolic links is not supported."
            echo_indent "You can manually add the following in $path"
            echo_indent "$code"
            return
        fi
        local text
        set +e
        text=$(cat "$path" | grep "$set_path")
        set -e
        if [[ "$text" != "" ]]; then
            # CLC PATH is already exported in this file
            log_debug "CLC PATH is already installed in $path"
            return
        fi
    fi
    # Add the CLC PATH to this file
    printf '\n# Added by Hazelcast CLC installer' >> "$path"
    printf "$code" >> "$path"
    log_info "Added CLC path to $path"
}

mv_path () {
    log_debug "MOVE $1 to $2"
    mv "$1" "$2"
}

detect_httpget () {
    if [[ "${state_curl_ok}" == "yes" ]]; then
        state_httpget=do_curl
    elif [[ "${state_wget_ok}" == "yes" ]]; then
        state_httpget=do_wget
    else
        bye "either curl or wget is required"
    fi
    log_debug "state_httpget=$state_httpget"
}

httpget () {
    log_debug "GET ${state_httpget} $1"
    ${state_httpget} "$@"
}

print_banner () {
    echo
    echo "Hazelcast CLC Installer (c) 2023 Hazelcast, Inc."
    echo
}

print_success () {
    echo
    echo_ok     "Hazelcast CLC ${state_download_version} is installed at $CLC_HOME"
    echo
    echo_indent 'Next steps:'
    echo_indent '1.  Open a new terminal,'
    echo_indent '2.  Run `clc version` to confirm that CLC is installed,'
    echo_indent '3.  Enjoy!'
    maybe_print_old_clc_warning
    echo
    echo_note   'If the steps above do not work, try copying `clc` binary to your $PATH:'
    echo_indent "$ sudo cp $state_bin_dir/clc /usr/local/bin"
    echo
}

maybe_print_old_clc_warning () {
    # create and assign the variable separately
    # so the exit status is not lost
    local clc_path
    set +e
    clc_path=$(which clc)
    set -e
    local bin_path="$state_bin_dir/clc"
    if [[ "$clc_path" != "" && "$clc_path" != "$bin_path" ]]; then
        echo
        echo_note   "A binary named 'clc' already exists at ${clc_path}."
        echo_indent 'You may want to delete it before running the installed CLC.'
        echo_indent "$ sudo rm -f ${clc_path}"
    fi
}

detect_last_release () {
    if [[ "$state_awk_ok"  != "yes" ]]; then
        bye "Awk is required for install"
    fi
    local re
    local text
    local v
    re='$1 ~ /tag_name/ { gsub(/[",]/, "", $2); print($2) }'
    text="$(httpget https://api.github.com/repos/hazelcast/hazelcast-commandline-client/releases)"
    if [[ "$state_beta" == "yes" ]]; then
        set +e
        v=$(echo "$text" | awk "$re" | head -1)
        set -e
    else
        set +e
        v=$(echo "$text" | awk "$re" | grep -vi preview | grep -vi beta | head -1)
        set -e
    fi
    if [[ "$v" == "" ]]; then
        bye "could not determine the latest version"
    fi
    state_download_version="$v"
    log_debug "state_download_version=$state_download_version"
}

detect_platform () {
    local os
    os="$(uname -s)"
    case "$os" in
        Linux*) os=linux; ext="tar.gz";;
        Darwin*) os=darwin; ext="zip";;
        *) bye "This script supports only Linux and MacOS, not $os";;
    esac
    state_os=$os
    log_debug "state_os=$state_os"
    state_archive_ext=$ext
    arch="$(uname -m)"
    case "$arch" in
        x86_64*) arch=amd64;;
        amd64*) arch=amd64;;
        armv6l*) arch=arm;;
        armv7l*) arch=arm;;
        arm64*) arch=arm64;;
        aarch64*) arch=arm64;;
        *) bye "This script supports only 64bit Intel and 32/64bit ARM architecture, not $arch"
    esac
    state_arch="$arch"
    log_debug "state_arch=$state_arch"
}

make_download_url () {
    local v=${state_download_version}
    local clc_name=${state_clc_name}
    local ext=${state_archive_ext}
    state_download_url="https://github.com/hazelcast/hazelcast-commandline-client/releases/download/$v/${clc_name}.${ext}"
}

make_clc_name () {
    local v="${state_download_version}"
    local os="${state_os}"
    local arch="${state_arch}"
    state_clc_name="hazelcast-clc_${v}_${os}_${arch}"
}

create_home () {
    log_info "Creating the Home directory: $CLC_HOME"
    mkdir -p "$state_bin_dir" "$CLC_HOME/etc"
    echo "install-script" > "$CLC_HOME/etc/.source"
}

download_release () {
    detect_tmpdir
    detect_platform
    detect_uncompress
    detect_last_release
    make_clc_name
    make_download_url
    log_info "Downloading: ${state_download_url}"
    local tmp
    local ext
    tmp="${state_tmp_dir}"
    ext="${state_archive_ext}"
    state_archive_path="$tmp/clc.${ext}"
    httpget "${state_download_url}" > "${state_archive_path}"
}

process_flags () {
    for flag in "$@"; do
        case "$flag" in
          --beta*) state_beta=yes;;
          --debug*) state_debug=yes;;
      	  --help*) print_banner; print_usage;;
          *) bye "Unknown option: $flag";;
        esac
    done
}

DEPENDENCIES="awk bash curl tar unzip wget xattr zsh"
CLC_HOME="${CLC_HOME:-$HOME/.hazelcast}"

state_arch=
state_archive_ext=
state_archive_path=
state_beta=no
state_bin_dir="$CLC_HOME/bin"
state_clc_name=
state_debug=no
state_download_url=
state_download_version=
state_httpget=
state_os=
state_tmp_dir=
state_uncompress=

state_awk_ok=no
state_curl_ok=no
state_tar_ok=no
state_unzip_ok=no
state_wget_ok=no
state_xattr_ok=no
state_bash_ok=no
state_zsh_ok=no

process_flags "$@"
print_banner
setup
create_home
download_release
install_release
update_config_files
print_success
