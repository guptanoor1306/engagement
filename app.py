import streamlit as st
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration

# --------------------------- Configuration ---------------------------

# Must be the first Streamlit command in your script:
st.set_page_config(layout="wide")

# 1. YouTube API Key from Streamlit Secrets
# In your Streamlit secrets.toml, include:
# [youtube]
# api_key = "YOUR_YOUTUBE_API_KEY"
API_KEY = st.secrets["youtube"]["api_key"]

# 2. List of Channel IDs to Track (your nine channels)
CHANNEL_IDS = [
    "UC415bOPUcGSamy543abLmRA",
    "UCRzYN32xtBf3Yxsx5BvJWJw",
    "UCVOTBwF0vnSxMRIbfSE_K_g",
    "UCPk2s5c4R_d-EUUNvFFODoA",
    "UCwAdQUuPT6laN-AQR17fe1g",
    "UCA295QVkf9O1RQ8_-s3FVXg",
    "UCkw1tYo7k8t-Y99bOXuZwhg",
    "UCxgAuX3XZROujMmGphN_scA",
    "UCUUlw3anBIkbW9W44Y-eURw",
]

# Global data store and flags
shorts_data = {}  # {video_id: [(timestamp, viewCount, likeCount, commentCount), ...]}
data_lock = threading.Lock()
no_shorts_flag = False
error_message = None

# ----------------------- Helper Functions ----------------------------

@st.cache_resource
def get_youtube_client():
    """Initialize and cache the YouTube Data API client."""
    return build("youtube", "v3", developerKey=API_KEY)


def iso8601_to_seconds(duration_str: str) -> int:
    """
    Convert an ISO 8601 duration (e.g., "PT45S") into total seconds.
    Uses isodate.parse_duration under the hood.
    """
    try:
        duration = parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception:
        return 0


def get_midnight_ist_utc() -> datetime:
    """
    Return the UTC datetime corresponding to “today’s midnight in IST.”
    IST = UTC + 5:30
    So 00:00 IST = 18:30 UTC (previous day).
    """
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    today_ist_date = now_ist.date()
    midnight_ist = datetime(today_ist_date.year, today_ist_date.month, today_ist_date.day, 0, 0)
    # Convert that to UTC
    midnight_utc = midnight_ist - timedelta(hours=5, minutes=30)
    return midnight_utc


def is_within_today(published_at_str: str) -> bool:
    """
    Given a video's publishedAt string (ISO 8601, UTC), return True if it falls within “today in IST.”
    """
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(hours=24)
    return midnight_utc <= pub_dt < next_midnight_utc


def discover_today_shorts() -> list:
    """
    Discover all Shorts (duration <= 180 s) published “today in IST” for each channel in CHANNEL_IDS.
    Returns a list of video IDs.
    """
    youtube = get_youtube_client()
    today_shorts = []

    for channel_id in CHANNEL_IDS:
        # 1. Get the channel’s “uploads” playlist
        ch_resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
        uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # 2. Page through the uploads playlist, 50 items at a time
        pl_req = youtube.playlistItems().list(
            part="snippet", playlistId=uploads_playlist, maxResults=50
        )

        while pl_req:
            pl_resp = pl_req.execute()
            for item in pl_resp["items"]:
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]

                if not is_within_today(published_at):
                    continue

                # Fetch duration to confirm it's <= 180 seconds
                cd_resp = youtube.videos().list(part="contentDetails", id=vid_id).execute()
                duration_str = cd_resp["items"][0]["contentDetails"]["duration"]
                duration_secs = iso8601_to_seconds(duration_str)
                if duration_secs <= 180:
                    today_shorts.append(vid_id)

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

    return today_shorts


def poll_stats_hourly():
    """
    Background thread function:
    - Discovers all today's Shorts.
    - If no Shorts, set no_shorts_flag.
    - If quota or API error, set error_message.
    - Otherwise, polls stats every hour.
    """
    global no_shorts_flag, error_message

    youtube = get_youtube_client()

    try:
        today_shorts = discover_today_shorts()
    except HttpError as e:
        error_message = f"YouTube API Error: {e}"
        return

    if not today_shorts:
        no_shorts_flag = True
        return

    # Initialize data structure
    with data_lock:
        for vid in today_shorts:
            shorts_data.setdefault(vid, [])

    # Poll hourly
    while True:
        now_ts = datetime.utcnow().isoformat() + "Z"
        for i in range(0, len(today_shorts), 50):
            batch_ids = today_shorts[i: i + 50]
            try:
                stats_resp = youtube.videos().list(
                    part="statistics", id=",".join(batch_ids)
                ).execute()
            except HttpError as e:
                error_message = f"YouTube API Error: {e}"
                return

            with data_lock:
                for vid in stats_resp["items"]:
                    stats = vid["statistics"]
                    row = (
                        now_ts,
                        int(stats.get("viewCount", 0)),
                        int(stats.get("likeCount", 0)),
                        int(stats.get("commentCount", 0)),
                    )
                    shorts_data[vid].append(row)

        now_dt = datetime.utcnow()
        seconds_til_next_hour = 3600 - (now_dt.minute * 60 + now_dt.second)
        time.sleep(seconds_til_next_hour)


@st.cache_resource
def start_background_thread():
    """
    Start the polling thread (only once per Streamlit session).
    """
    thread = threading.Thread(target=poll_stats_hourly, daemon=True)
    thread.start()
    return thread


# ----------------------------- Main UI -------------------------------

# Kick off the polling thread (only on first run)
start_background_thread()

# 1. If there is an API error, show it
if error_message:
    st.error(error_message)
    st.stop()

# 2. If no Shorts were uploaded today, show that message
if no_shorts_flag:
    st.info("No Shorts (<= 3 minutes) were uploaded today for the selected channels.")
    st.stop()

# 3. Otherwise, if discovery is still in progress, wait
with data_lock:
    all_videos = list(shorts_data.keys())

if not all_videos:
    st.info("Waiting for background thread to discover today’s Shorts and capture stats…")
    st.stop()

# 4. If we have data, show the dashboard
st.title("📊 YouTube Shorts VPH & Engagement Tracker")

# Sidebar: pick which video to inspect
selected_vid = st.sidebar.selectbox("Select a video ID:", all_videos)

# Build a DataFrame of that video’s stats
with data_lock:
    stats_rows = shorts_data[selected_vid].copy()

if not stats_rows:
    st.warning("No stats captured yet for this video. Please wait a few minutes.")
    st.stop()

df = pd.DataFrame(
    stats_rows, columns=["timestamp", "viewCount", "likeCount", "commentCount"]
)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["vph"] = df["viewCount"].diff().fillna(0)
df["engagement_rate"] = (df["likeCount"] + df["commentCount"]) / df["viewCount"]

# Display the latest metrics
st.subheader("Latest Metrics")
latest = df.iloc[-1]
st.markdown(f"""
- **Timestamp (UTC):** {latest['timestamp']}
- **Total Views:** {int(latest['viewCount'])}
- **Views Per Hour (VPH):** {int(latest['vph'])}
- **Approximate Engagement Rate:** {latest['engagement_rate']:.2%}
""")

# Plot VPH over time
st.subheader("VPH Over Time")
vph_chart = df.set_index("timestamp")["vph"]
st.line_chart(vph_chart)

# Plot Engagement Rate over time
st.subheader("Engagement Rate Over Time")
eng_chart = df.set_index("timestamp")["engagement_rate"]
st.line_chart(eng_chart)

# Show raw data
st.subheader("Raw Data Table")
st.dataframe(df.reset_index(drop=True), use_container_width=True)
