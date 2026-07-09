# =============================================================================
# Project 4 – Enterprise Multi-Source ETL Pipeline
# Notebook: 01_sftp_pgp_ingestion.py
#
# Purpose : Download PGP-encrypted files from SFTP server, decrypt using key
#           stored in Azure Key Vault, and land raw data on ADLS Gen2 for
#           downstream Databricks processing.
#
# Stack   : Azure Databricks | Python | PGP/GPG | Azure Key Vault | ADLS Gen2
# =============================================================================

# %pip install pgpy paramiko azure-keyvault-secrets azure-identity

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import paramiko
import pgpy
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import io, os, logging
from datetime import datetime

logger = logging.getLogger("sftp_pgp_ingestion")
spark  = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION — use Key Vault references / Databricks Secrets in production
# ---------------------------------------------------------------------------
KEY_VAULT_URL      = "https://<your-keyvault>.vault.azure.net/"
SFTP_HOST_SECRET   = "sftp-hostname"
SFTP_USER_SECRET   = "sftp-username"
SFTP_PASS_SECRET   = "sftp-password"      # or use SSH key secret
PGP_PRIVKEY_SECRET = "pgp-private-key"
PGP_PASSPHRASE_SECRET = "pgp-passphrase"

SFTP_REMOTE_DIR    = "/outbound/data/"
LOCAL_STAGING_DIR  = "/tmp/sftp_staging/"
ADLS_LANDING_PATH  = "abfss://raw@<storage>.dfs.core.windows.net/sftp-ingest/"

FILE_TRACKING_TABLE = "catalog.control.sftp_file_tracking"

# ---------------------------------------------------------------------------
# AZURE KEY VAULT — retrieve secrets
# ---------------------------------------------------------------------------

def get_secret(secret_name: str) -> str:
    """Retrieve a secret value from Azure Key Vault using managed identity."""
    credential = DefaultAzureCredential()
    client     = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    return client.get_secret(secret_name).value


# In Databricks, use dbutils.secrets instead:
def get_secret_dbutils(scope: str, key: str) -> str:
    """Preferred Databricks pattern: read secrets from Databricks secret scope."""
    return dbutils.secrets.get(scope=scope, key=key)

# ---------------------------------------------------------------------------
# SFTP CONNECTION
# ---------------------------------------------------------------------------

def create_sftp_client(host: str, username: str, password: str) -> paramiko.SFTPClient:
    """Open an SFTP connection and return the SFTP client."""
    transport = paramiko.Transport((host, 22))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


def list_sftp_files(sftp: paramiko.SFTPClient, remote_dir: str, extension: str = ".pgp") -> list:
    """List all files in the remote directory matching the extension."""
    return [
        f.filename
        for f in sftp.listdir_attr(remote_dir)
        if f.filename.endswith(extension)
    ]


def download_file(sftp: paramiko.SFTPClient, remote_path: str, local_path: str) -> None:
    """Download a single file from SFTP to local staging."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    sftp.get(remote_path, local_path)
    logger.info(f"Downloaded: {remote_path} → {local_path}")

# ---------------------------------------------------------------------------
# PGP DECRYPTION
# ---------------------------------------------------------------------------

def decrypt_pgp_file(encrypted_path: str, private_key_str: str, passphrase: str) -> bytes:
    """
    Decrypt a PGP-encrypted file using the private key from Key Vault.
    Returns the decrypted file bytes.
    """
    private_key, _ = pgpy.PGPKey.from_blob(private_key_str)

    with private_key.unlock(passphrase):
        with open(encrypted_path, "rb") as f:
            encrypted_msg = pgpy.PGPMessage.from_blob(f.read())
        decrypted = private_key.decrypt(encrypted_msg)

    return decrypted.message.encode("utf-8") if isinstance(decrypted.message, str) else decrypted.message

# ---------------------------------------------------------------------------
# UPLOAD DECRYPTED FILE TO ADLS GEN2
# ---------------------------------------------------------------------------

def upload_to_adls(content: bytes, adls_path: str) -> None:
    """Write decrypted file content to ADLS Gen2 using Databricks DBFS or SDK."""
    # Using dbutils to write to ADLS:
    local_tmp = f"/tmp/decrypted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.dat"
    with open(local_tmp, "wb") as f:
        f.write(content)
    dbutils.fs.cp(f"file://{local_tmp}", adls_path)
    os.remove(local_tmp)
    logger.info(f"Uploaded to ADLS: {adls_path}")

# ---------------------------------------------------------------------------
# FILE TRACKING
# ---------------------------------------------------------------------------

def update_file_status(filename: str, status: str, error: str = None) -> None:
    """Track ingestion status per file in Delta control table."""
    row = [{
        "filename":   filename,
        "status":     status,    # DOWNLOADED / DECRYPTED / UPLOADED / FAILED
        "error":      error or "",
        "updated_at": datetime.utcnow().isoformat()
    }]
    spark.createDataFrame(row).write \
         .format("delta").mode("append") \
         .saveAsTable(FILE_TRACKING_TABLE)

# ---------------------------------------------------------------------------
# MAIN INGESTION PIPELINE
# ---------------------------------------------------------------------------

def run_sftp_pgp_pipeline():
    logger.info("Starting SFTP PGP ingestion pipeline …")

    # Retrieve secrets
    sftp_host     = get_secret_dbutils("my-scope", SFTP_HOST_SECRET)
    sftp_user     = get_secret_dbutils("my-scope", SFTP_USER_SECRET)
    sftp_pass     = get_secret_dbutils("my-scope", SFTP_PASS_SECRET)
    pgp_privkey   = get_secret_dbutils("my-scope", PGP_PRIVKEY_SECRET)
    pgp_passphrase = get_secret_dbutils("my-scope", PGP_PASSPHRASE_SECRET)

    sftp = create_sftp_client(sftp_host, sftp_user, sftp_pass)
    files = list_sftp_files(sftp, SFTP_REMOTE_DIR)
    logger.info(f"Found {len(files)} PGP files on SFTP.")

    for filename in files:
        try:
            remote_path = f"{SFTP_REMOTE_DIR}{filename}"
            local_path  = f"{LOCAL_STAGING_DIR}{filename}"

            # Download
            download_file(sftp, remote_path, local_path)
            update_file_status(filename, "DOWNLOADED")

            # Decrypt
            decrypted_bytes = decrypt_pgp_file(local_path, pgp_privkey, pgp_passphrase)
            update_file_status(filename, "DECRYPTED")

            # Upload to ADLS
            plain_name = filename.replace(".pgp", "")
            run_date   = datetime.utcnow().strftime("%Y/%m/%d")
            adls_path  = f"{ADLS_LANDING_PATH}{run_date}/{plain_name}"
            upload_to_adls(decrypted_bytes, adls_path)
            update_file_status(filename, "UPLOADED")

            # Clean up local temp files
            os.remove(local_path)

        except Exception as e:
            logger.error(f"Failed processing {filename}: {e}")
            update_file_status(filename, "FAILED", error=str(e))

    sftp.close()
    logger.info("SFTP PGP ingestion pipeline complete.")


if __name__ == "__main__":
    run_sftp_pgp_pipeline()
