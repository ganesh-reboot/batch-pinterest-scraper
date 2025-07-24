import streamlit as st
import authlib
import uuid
from google.cloud import batch_v1
from google.protobuf import duration_pb2
import json
from google.oauth2 import service_account

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



def submit_job(input_strings):
    client = batch_v1.BatchServiceClient(credentials=credentials)
    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    job_id = f"scraper-job-{uuid.uuid4().hex[:8]}"

    runnable = batch_v1.Runnable()
    runnable.container.image_uri = IMAGE_URI
    runnable.container.entrypoint = "python"
    runnable.container.commands = ["scraper.py"] + input_strings

    task_spec = batch_v1.TaskSpec(
        runnables=[runnable],
        max_run_duration=duration_pb2.Duration(seconds=7200)
    )

    task_group = batch_v1.TaskGroup(task_spec=task_spec)

    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [
        batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
            policy=batch_v1.AllocationPolicy.InstancePolicy(
                machine_type="e2-small"
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


# Constants (replace with your actual values)
PROJECT_ID = st.secrets.gcp.project_id
REGION = st.secrets.gcp.region
IMAGE_URI = st.secrets.gcp.image_uri

st.set_page_config(page_title="Google Auth + Batch Job")

st.title("🔐 Authenticated Job Submitter")

# Handle login
if not st.user.is_logged_in:
    st.warning("You must be logged in to submit a job.")
    if st.button("Log in with Google", type="primary", icon=":material/login:"):
        st.login()
    st.stop()

# Authenticated UI
st.success(f"Hello, **{st.user.name}**! You are logged in.")

if st.button("Log out", type="secondary", icon=":material/logout:"):
    st.logout()
    st.stop()

# Collect inputs from the user
st.subheader("🧾 Submit a Job")
input_text = st.text_input("Enter input strings (comma-separated):")

st.header("🏃 Running jobs")
running = list_running_jobs_for_user(st.user.email)
for job in running:
    st.info(f"🟢 {job.name} - {job.status.state.name}")

# 3. Past jobs
st.header("📁 Completed jobs")
past = list_completed_jobs_for_user(st.user.email)
for job in past:
    st.success(f"✅ {job.name} - {job.status.state.name}")

if st.button("🚀 Submit Job"):
    input_strings = [s.strip() for s in input_text.split(",") if s.strip()]
    if not input_strings:
        st.error("Please enter at least one input string.")
    else:
        with st.spinner("Submitting job..."):
            try:
                job_name = submit_job(input_strings)
                st.success(f"✅ Job submitted: `{job_name}`")
            except Exception as e:
                st.error("❌ Failed to submit job.")
                st.exception(e)
