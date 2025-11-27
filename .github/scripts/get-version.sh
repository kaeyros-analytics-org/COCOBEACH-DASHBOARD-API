#!/bin/bash
set -e

MANUAL_VERSION="${1:-}"
VERSION_BUMP="${2:-patch}"

get_latest_version() {
    local latest_tag=$(git tag -l "v*" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n1)
    
    if [[ -z "$latest_tag" ]]; then
        echo "v0.0.0"
    else
        echo "$latest_tag"
    fi
}

validate_version() {
    local new_version=$1
    local latest_version=$2
    
    IFS='.' read -r -a latest_parts <<< "$latest_version"
    IFS='.' read -r -a new_parts <<< "$new_version"
    
    if [[ ${new_parts[0]} -lt ${latest_parts[0]} ]] || \
       [[ ${new_parts[0]} -eq ${latest_parts[0]} && ${new_parts[1]} -lt ${latest_parts[1]} ]] || \
       [[ ${new_parts[0]} -eq ${latest_parts[0]} && ${new_parts[1]} -eq ${latest_parts[1]} && ${new_parts[2]} -le ${latest_parts[2]} ]]; then
        echo "❌ Error: Version $new_version is not greater than latest version $latest_version"
        exit 1
    fi
}

increment_version() {
    local version=$1
    local bump_type=$2
    
    IFS='.' read -r -a parts <<< "$version"
    local major="${parts[0]:-0}"
    local minor="${parts[1]:-0}"
    local patch="${parts[2]:-0}"
    
    case "$bump_type" in
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        *)
            patch=$((patch + 1))
            ;;
    esac
    
    echo "${major}.${minor}.${patch}"
}

main() {
    local latest_tag=$(get_latest_version)
    local latest_version=${latest_tag#v}
    
    echo "Latest tag (sorted): $latest_tag"
    echo "Latest version: $latest_version"
    
    local new_version
    
    if [[ -n "$MANUAL_VERSION" ]]; then
        new_version="$MANUAL_VERSION"
        echo "Using manual version: $new_version"
        validate_version "$new_version" "$latest_version"
    else
        new_version=$(increment_version "$latest_version" "$VERSION_BUMP")
        echo "Auto-incremented version ($VERSION_BUMP): $new_version"
    fi
    
    echo "NEW_VERSION=$new_version" >> $GITHUB_OUTPUT
    echo "LATEST_VERSION=$latest_version" >> $GITHUB_OUTPUT
    echo "✅ Version validated: $latest_version → $new_version"
}

main
