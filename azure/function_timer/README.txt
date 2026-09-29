# Azure Functions (Python v2 is optional; this uses the v1 folder model).
# Deploy the function_timer/ folder and install the package under src/.
#
# Local run (requires Azure Functions Core Tools):
#   func start
#
# App settings / env vars to set (Key Vault references recommended):
#   FS_BASE_URL, FS_API_KEY
#   AOAI_ENDPOINT, AOAI_API_KEY, AOAI_EMBED_DEPLOYMENT, EMBED_DIMENSIONS
#   SEARCH_ENDPOINT, SEARCH_API_KEY, SEARCH_INDEX_NAME
#   STATE_DIR=/home/site/data/.state   # persist the watermark outside the app dir
notes: see ../README.md and ../../README.md
