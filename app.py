import streamlit as st
import authlib
import uuid
from google.cloud import batch_v1
from google.protobuf import duration_pb2
import json
from google.oauth2 import service_account
import pandas as pd
from google.cloud import storage
import io
from datetime import datetime

creds_dict = json.loads(st.secrets["gcp"]["google_credentials"])
credentials = service_account.Credentials.from_service_account_info(creds_dict)

def list_running_jobs_for_user(user_email):
    client = batch_v1.BatchServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"

    # Fetch all jobs
    all_jobs = client.list_jobs(parent=parent)

    user_label = user_email.replace("@", "_at_").replace(".", "")
    running_jobs = [
        job for job in all_jobs
        if job.labels.get("user") == user_label and
           job.status.state in [batch_v1.JobStatus.State.RUNNING, batch_v1.JobStatus.State.QUEUED]
    ]

    return running_jobs

def list_completed_jobs_for_user(user_email):
    client = batch_v1.BatchServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"

    all_jobs = client.list_jobs(parent=parent)

    user_label = user_email.replace("@", "_at_").replace(".", "")
    completed_jobs = [
        job for job in all_jobs
        if job.labels.get("user") == user_label and
           job.status.state in [batch_v1.JobStatus.State.SUCCEEDED, batch_v1.JobStatus.State.FAILED]
    ]

    return completed_jobs



def submit_job(email, input_strings):
    client = batch_v1.BatchServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    job_id = f"job-{input_strings[0].strip().lower().replace(' ', '-')}-{timestamp}"

    runnable = batch_v1.Runnable()
    runnable.container.image_uri = IMAGE_URI
    runnable.container.entrypoint = "python"
    runnable.container.commands = ["scraper.py"] + [email] + input_strings

    task_spec = batch_v1.TaskSpec(
        runnables=[runnable],
        max_run_duration=duration_pb2.Duration(seconds=54000)
    )

    task_group = batch_v1.TaskGroup(task_spec=task_spec)

    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [
        batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
            policy=batch_v1.AllocationPolicy.InstancePolicy(
                machine_type="e2-micro"
            )
        )
    ]

    job = batch_v1.Job(
        task_groups=[task_group],
        allocation_policy=allocation_policy,
        labels={
        "env": "prod",
        "user": st.user.email.replace("@", "_at_").replace(".", "")
    },
        logs_policy=batch_v1.LogsPolicy(
            destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        )
    )

    response = client.create_job(parent=parent, job=job, job_id=job_id)
    return response.name

PROJECT_ID = st.secrets.gcp.project_id
REGION = st.secrets.gcp.region
IMAGE_URI = st.secrets.gcp.image_uri
BUCKET_NAME = st.secrets.gcp.bucket_name  # Make sure you have this in secrets.toml

# --- PAGE SETUP ---
st.set_page_config(page_title="Google Auth + Batch Job")
st.title("Pinterest Scraper")

# --- AUTH ---
if not st.user.is_logged_in:
    st.warning("You must be logged in to submit a job.")
    if st.button("Log in with Google", type="primary", icon=":material/login:"):
        st.login()
    st.stop()

st.success(f"Hello, **{st.user.name}**!")

if st.button("Log out", type="secondary", icon=":material/logout:"):
    st.logout()
    st.stop()

user_email_safe = st.user.email.replace("@", "_at_").replace(".", "")

# --- INPUT ---
st.subheader("Submit a scraping job")
input_text = st.text_area("Enter one keyword per line:")

if st.button("Start Scraper"):
    input_strings = [s.strip() for s in input_text.splitlines() if s.strip()]
    if not input_strings:
        st.error("Please enter at least one input string.")
    else:
        with st.spinner("Submitting job..."):
            try:
                job_name = submit_job(user_email_safe, input_strings)
                st.success(f"✅ Job submitted: `{job_name}`")
            except Exception as e:
                st.error("❌ Failed to submit job.")
                st.exception(e)

# --- JOB TABLE ---
st.header("Job Status")
try:
    running = list_running_jobs_for_user(st.user.email)
    completed = list_completed_jobs_for_user(st.user.email)

    all_jobs = running + completed
    job_data = [
        {"Job Name": job.name.split('/')[-1], "Status": job.status.state.name}
        for job in all_jobs
    ]

    if job_data:
        st.dataframe(pd.DataFrame(job_data), use_container_width=True)
    else:
        st.info("No jobs found.")
except Exception as e:
    st.error("Failed to fetch job status.")
    st.exception(e)

# --- GCS FILE TABLE ---
st.header("Job Results")
client = storage.Client()
bucket_name = st.secrets.gcp.bucket_name  # or hardcode your bucket name
bucket = client.bucket(bucket_name)

# Convert email to match file naming convention
user_prefix = st.user.email.replace("@", "_at_").replace(".", "")

# List CSV result files for this user
blobs = list(client.list_blobs(bucket, prefix=f"{user_prefix}/"))
csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]

if not csv_files:
    st.info("No result files found yet.")
else:
    for file_name in sorted(csv_files, reverse=True):
        with st.expander(f"📂 {file_name.split('/')[-1]}"):
            blob = bucket.blob(file_name)
            file_bytes = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(file_bytes))

            st.dataframe(df, use_container_width=True)

            st.download_button(
                label="⬇️ Download CSV",
                data=file_bytes,
                file_name=file_name.split("/")[-1],
                mime="text/csv"
            )