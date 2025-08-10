# === helper_decode_sa.py ===
# Use this snippet in your startup code to materialize the Google SA JSON from the env var.
import base64, json, os, tempfile, pathlib

def write_sa_json_from_env(env_var="GOOGLE_SA_JSON_B64") -> str:
    b64 = os.getenv(env_var, "").strip()
    if not b64:
        raise RuntimeError(f"{env_var} is not set")
    data = base64.b64decode(b64)
    # validate json
    obj = json.loads(data)
    # write to a secure temp file
    tmp = tempfile.NamedTemporaryFile(prefix="sa_", suffix=".json", delete=False)
    tmp.write(data); tmp.flush(); tmp.close()
    # set a pointer env var used by google-auth libs
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
    return tmp.name

if __name__ == "__main__":
    path = write_sa_json_from_env()
    print("Service account JSON written to:", path)
