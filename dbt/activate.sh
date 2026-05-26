
DBT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$DBT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "dbt/activate.sh: $ENV_FILE not found." >&2
  echo "  Run: cp dbt/.env.example dbt/.env  and fill in values." >&2
  return 1 2>/dev/null || exit 1
fi

set -a
source "$ENV_FILE"
set +a

export DBT_PROFILES_DIR="$DBT_DIR"

export DBT_PROJECT_DIR="$DBT_DIR"

if [ -d "$DBT_DIR/.venv-dbt/bin" ]; then
  export PATH="$DBT_DIR/.venv-dbt/bin:$PATH"
fi

cd "$DBT_DIR" || return 1

echo "dbt env active:"
echo "  DBT_PROFILES_DIR = $DBT_PROFILES_DIR"
echo "  cwd              = $(pwd)"
echo "  dbt binary       = $(command -v dbt || echo '(not found — did you create .venv-dbt and pip install?)')"
