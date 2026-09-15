#!/usr/bin/env bash
#
# Build Crucible images from the current checkout and push them to ECR.
#
#   ./scripts/build_push.sh worker              # one component
#   ./scripts/build_push.sh worker mcp          # several
#   ./scripts/build_push.sh all                 # worker + control-plane + mcp
#   ./scripts/build_push.sh --allow-dirty all   # tag an uncommitted tree
#   ./scripts/build_push.sh --latest all        # also move the :latest tag
#
# Images are tagged with the short commit SHA.  That tag is what you deploy — it
# pins the running pods to a commit you can check out, which a floating :latest
# cannot do.  The script refuses to build from a dirty tree unless
# --allow-dirty, because a SHA tag on uncommitted code is a lie.
#
# :latest is NOT pushed by default.  values.yaml defaults every image to
# `tag: latest` with `pullPolicy: Always`, so moving that tag silently upgrades
# any release that was not pinned to a SHA — the next reschedule of any pod
# pulls the new build, with no Helm revision recording it.  Pass --latest only
# when you mean to move that tag for everyone.
#
# Notes:
#   - Images are built --platform linux/amd64: EKS nodes are amd64 and a Mac
#     builds arm64 by default, which fails at runtime with an exec format error.
#   - Build and push are separate steps (no `buildx --push`): the claude-bot
#     role lacks ecr:BatchGetImage, which buildx needs to push.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AWS_PROFILE_NAME="claude-bot"
AWS_REGION="ap-southeast-1"

KNOWN_COMPONENTS="worker control-plane mcp"

# Component metadata as "ecr-repo:dockerfile:helm-values-key".  A case statement
# rather than an associative array: macOS still ships bash 3.2, which has none.
component_meta() {
    case "$1" in
        worker)        echo "crucible/worker:worker/Dockerfile:worker.image.tag" ;;
        control-plane) echo "crucible/control-plane:control_plane/Dockerfile:controlPlane.image.tag" ;;
        mcp)           echo "crucible/mcp:mcp_server/Dockerfile:mcp.image.tag" ;;
        *)             return 1 ;;
    esac
}

ALLOW_DIRTY=false
PUSH_LATEST=false
SELECTED=()

for arg in "$@"; do
    case "$arg" in
        --allow-dirty) ALLOW_DIRTY=true ;;
        --latest)      PUSH_LATEST=true ;;
        all)           SELECTED+=(worker control-plane mcp) ;;
        -h|--help)     sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        -*)            echo "Unknown flag: $arg" >&2; exit 2 ;;
        *)
            if ! component_meta "$arg" > /dev/null; then
                echo "Unknown component: $arg (known: $KNOWN_COMPONENTS, all)" >&2
                exit 2
            fi
            SELECTED+=("$arg")
            ;;
    esac
done

if [[ ${#SELECTED[@]} -eq 0 ]]; then
    echo "Usage: $0 [--allow-dirty] [--latest] <worker|control-plane|mcp|all>..." >&2
    exit 2
fi

# Drop duplicates so `all worker` doesn't build the worker image twice.
DEDUPED=()
for component in "${SELECTED[@]}"; do
    seen=false
    for existing in ${DEDUPED[@]+"${DEDUPED[@]}"}; do
        [[ "$existing" == "$component" ]] && seen=true && break
    done
    [[ "$seen" == "false" ]] && DEDUPED+=("$component")
done
SELECTED=("${DEDUPED[@]}")

cd "$REPO_ROOT"

# ── Resolve the tag ──────────────────────────────────────────────────────────
TAG="$(git rev-parse --short HEAD)"
if [[ -n "$(git status --porcelain)" ]]; then
    if [[ "$ALLOW_DIRTY" != "true" ]]; then
        echo "ERROR: working tree is dirty — '$TAG' would not describe what you built." >&2
        echo "       Commit first, or re-run with --allow-dirty." >&2
        exit 1
    fi
    TAG="${TAG}-dirty"
    echo "WARNING: building from a dirty tree; tagging as $TAG"
fi

# ── Registry + login ─────────────────────────────────────────────────────────
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text --profile "$AWS_PROFILE_NAME")"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "==> Logging in to $REGISTRY"
aws ecr get-login-password --region "$AWS_REGION" --profile "$AWS_PROFILE_NAME" \
    | docker login --username AWS --password-stdin "$REGISTRY"

# ── Build + push ─────────────────────────────────────────────────────────────
for component in "${SELECTED[@]}"; do
    IFS=: read -r repo dockerfile _ <<< "$(component_meta "$component")"
    image="${REGISTRY}/${repo}"

    echo ""
    echo "==> Building $component ($dockerfile) -> ${image}:${TAG}"
    build_tags=(-t "${image}:${TAG}")
    if [[ "$PUSH_LATEST" == "true" ]]; then
        build_tags+=(-t "${image}:latest")
    fi
    docker build \
        --platform linux/amd64 \
        -f "$dockerfile" \
        "${build_tags[@]}" \
        .

    echo "==> Pushing ${image}:${TAG}"
    docker push "${image}:${TAG}"
    if [[ "$PUSH_LATEST" == "true" ]]; then
        echo "==> Pushing ${image}:latest"
        docker push "${image}:latest"
    fi
done

# ── Report ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  Pushed at tag: $TAG"
echo ""
echo "  Deploy with:"
printf '    helm upgrade crucible ./helm/crucible \\\n'
printf '      -f helm/crucible/values-eks.yaml \\\n'
printf '      --set awsAccountId=%s \\\n' "$ACCOUNT_ID"
for component in "${SELECTED[@]}"; do
    IFS=: read -r _ _ values_key <<< "$(component_meta "$component")"
    printf '      --set %s=%s \\\n' "$values_key" "$TAG"
done
printf '      --namespace crucible --kubeconfig ~/.kube/claude-config\n'
echo ""
echo "  (RabbitMQ/PostgreSQL passwords must also be passed on a fresh install.)"
echo "============================================"
