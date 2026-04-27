import json
import uuid
import random
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

# Connect to MongoDB Atlas cluster
client = MongoClient(
    "mongodb+srv://carolinal_db_user:zanahorita@cluster0.mr5yolt.mongodb.net/?retryWrites=true&w=majority",
    tls=True,
    tlsAllowInvalidCertificates=True
)

# Select database and collection where logs will be stored
db = client["provenanceDB"]
collection = db["logs"]

# =========================================================
# 1. OUTPUT DIRECTORY
# =========================================================
# Create a folder where all generated JSON provenance files will be saved

output_dir = "provenance_logs"
os.makedirs(output_dir, exist_ok=True)  # avoids error if folder already exists

# =========================================================
# 2. EXECUTION NODES
# =========================================================
# Simulating different compute nodes with different speeds and failure rates

nodes = {
    "atlas-qc-01": {"speed": "fast", "failure_rate": 0.02},
    "orion-align-02": {"speed": "medium", "failure_rate": 0.05},
    "kepler-variant-03": {"speed": "slow", "failure_rate": 0.12},
    "nova-storage-04": {"speed": "medium", "failure_rate": 0.04}
}

# List of node names (used later for random selection)
node_names = list(nodes.keys())

# =========================================================
# 3. USERS
# =========================================================
# Simulated users that execute the pipeline

users = ["salle_alumni", "bio_user", "lab_tech"]


# =========================================================
# 4. TIMESTAMP GENERATION
# =========================================================
# Function to generate realistic start and end times

def generate_times(duration_seconds):
    base_time = datetime(2026, 3, 31, 9, 0, 0)  # fixed starting reference
    offset = timedelta(minutes=random.randint(0, 180))  # random offset (0–3h)

    start = base_time + offset
    end = start + timedelta(seconds=duration_seconds)

    # Return timestamps in ISO format (with Z for UTC)
    return start.isoformat() + "Z", end.isoformat() + "Z"


# =========================================================
# 5. GENERATE ONE PROVENANCE RECORD
# =========================================================
# This function creates one simulated provenance log

def generate_log(i, previous_id=None):
    # -----------------------------
    # Select a random node
    # -----------------------------
    node_name = random.choice(node_names)
    node = nodes[node_name]

    # -----------------------------
    # Simulate FASTQ input data
    # -----------------------------
    file_count = random.randint(1, 4)  # number of input files
    size_per_file = random.randint(800_000_000, 1_500_000_000)  # ~0.8–1.5GB per file
    total_size = file_count * size_per_file

    # -----------------------------
    # Estimate execution duration
    # -----------------------------
    base_time = total_size / 10_000_000  # simple scaling rule

    # Adjust duration depending on node speed
    if node["speed"] == "fast":
        duration = int(base_time * random.uniform(0.5, 0.8))
    elif node["speed"] == "medium":
        duration = int(base_time * random.uniform(0.8, 1.2))
    else:  # slow node
        duration = int(base_time * random.uniform(1.2, 1.8))

    # Generate timestamps
    start, end = generate_times(duration)

    # -----------------------------
    # Simulate integrity checks
    # -----------------------------
    sha_ok = random.random() > node["failure_rate"]
    seqfu_ok = random.random() > node["failure_rate"]

    # Create detailed SHA output per file
    file_details = []
    for j in range(file_count):
        status = "OK" if sha_ok else random.choice(["OK", "FAIL"])
        file_details.append(f"sample{i}_R{j + 1}.fastq.gz: {status}")

    sha_value = " ".join(file_details)
    seqfu_value = "OK" if seqfu_ok else "ERROR"

    # Unique activity ID using UUID
    activity_id = f"urn:uuid:{uuid.uuid4()}"

    # =====================================================
    # JSON STRUCTURE (based on PROV standard)
    # =====================================================
    log = {
        "@context": "http://www.w3.org/ns/prov#",
        "@id": activity_id,
        "@type": "Activity",
        "label": f"Processament complet de sample_{i}",

        "startTime": start,
        "endTime": end,
        "executionNode": node_name,
        "sourceDirectory": "/data/input/",
        "destinationDirectory": f"/data/output/sample_{i}",

        # Agents involved in the process
        "wasAssociatedWith": [
            {
                "@type": "SoftwareAgent",
                "label": "seqfu",
                "version": "1.22.3"
            },
            {
                "@type": "SoftwareAgent",
                "label": "sha256sum",
                "version": "sha256sum (GNU coreutils) 8.32"
            },
            {
                "@type": "SoftwareAgent",
                "label": "Pipeline Nextflow fastq_prov",
                "repository": "local",
                "commitId": "N/A",
                "revision": "N/A"
            },
            {
                "@id": f"urn:person:{random.choice(users)}",
                "@type": "Person",
                # Including username in label for clarity
                "label": f"Usuari executor: {random.choice(users)}",
                "actedOnBehalfOf": {
                    "@id": "https://ror.org/01y990p52",
                    "@type": "Organization",
                    "label": "La Salle"
                }
            }
        ],

        # Outputs generated by the activity
        "generated": [
            {
                "@type": "Entity",
                "label": "Verificació SHA256",
                "description": "Resultat de la comprovació de checksum a destí",
                "value": sha_value,
                # Extra field added to simplify analysis
                "status": "OK" if sha_ok else "FAIL"
            },
            {
                "@type": "Entity",
                "label": "Verificació Seqfu",
                "description": "Resultat de la comprovació d'integritat del format FASTQ",
                "value": seqfu_value,
                "status": seqfu_value
            },
            {
                "@type": "Entity",
                "label": "FASTQ Files",
                # Keeping as string to match assignment format
                "totalSizeBytes": str(total_size),
                "category": "Genet",
                "fileCount": str(file_count)
            }
        ]
    }

    # =====================================================
    # EXTRA (JUSTIFIED EXTENSIONS)
    # =====================================================

    # Input data used by the activity
    log["used"] = [
        {
            "@type": "Entity",
            "label": "Input FASTQ",
            "totalSizeBytes": str(total_size),
            "fileCount": str(file_count)
        }
    ]

    # Optional dependency with previous activity (simulating workflow chaining)
    if previous_id and random.random() < 0.3:
        log["wasInformedBy"] = {
            "@id": previous_id
        }

    return log, activity_id


# =========================================================
# 6. GENERATE 100 FILES
# =========================================================
previous_id = None

for i in range(1, 101):
    # Generate one log
    log, current_id = generate_log(i, previous_id)

    # Save JSON file locally
    with open(f"{output_dir}/log_{i}.json", "w") as f:
        json.dump(log, f, indent=4, ensure_ascii=False)

    # Insert log into MongoDB collection
    collection.insert_one(log)

    # Keep track of previous ID for chaining
    previous_id = current_id

print("100 JSON files generated successfully.")