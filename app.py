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

# Global data store and control flags
shorts_data = {}  # {video_id: [(timestamp, viewCount, likeCount, commentCount), ...]}
video_to_channel = {}  # map video_id to channel title for UI display
data_lock = threading.Lock()
discovery_logs = []  # real-time logs of channel discovery
discovery_logs_lock = threading.Lock()
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
    """
    try:
        duration = parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception:
        return 0


def get_midnight_ist_utc() -> datetime:
    """
    Return the UTC datetime corresponding to “today’s midnight in IST.”
    """
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    today_ist_date = now_ist.date()
    midnight_ist = datetime(today_ist_date.year, today_ist_date.month, today_ist_date.day, 0, 0)
    midnight_utc = midnight_ist - timedelta(hours=5, minutes=30)
    return midnight_utc


def is_within_today(published_at_str: str) -> bool:
    """
    Return True if a video's publishedAt (UTC) falls within “today in IST.”
    """
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(hours=24)
    return midnight_utc <= pub_dt < next_midnight_utc


def poll_stats_hourly():
    """
    Background thread function:
    1. Discovers all today's Shorts (<= 3 minutes), logging per channel.
    2. If no Shorts, set no_shorts_flag and exit.
    3. If any API error, set error_message and exit.
    4. Otherwise, performs one immediate stats fetch, then polls every hour.
    """
    global no_shorts_flag, error_message

    youtube = get_youtube_client()
    today_shorts = []

    # 1. Discover per channel with real-time logging, including channel title
    for idx, channel_id in enumerate(CHANNEL_IDS, start=1):
        try:
            ch_resp = youtube.channels().list(part="snippet,contentDetails", id=channel_id).execute()
            channel_title = ch_resp["items"][0]["snippet"]["title"]
            uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        except HttpError as e:
            error_message = f"YouTube API Error (channel fetch): {e}"
            return

        with discovery_logs_lock:
            discovery_logs.append(f"Checking channel {idx}/{len(CHANNEL_IDS)}: {channel_title}")

        channel_shorts = []
        pl_req = youtube.playlistItems().list(part="snippet", playlistId=uploads_playlist, maxResults=50)
        while pl_req:
            try:
                pl_resp = pl_req.execute()
            except HttpError as e:
                error_message = f"YouTube API Error (playlistItems): {e}"
                return

            for item in pl_resp["items"]:
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]
                if not is_within_today(published_at):
                    continue

                try:
                    cd_resp = youtube.videos().list(part="contentDetails", id=vid_id).execute()
                except HttpError as e:
                    error_message = f"YouTube API Error (contentDetails): {e}"
                    return

                duration_str = cd_resp["items"][0]["contentDetails"]["duration"]
                duration_secs = iso8601_to_seconds(duration_str)
                if duration_secs <= 180:
                    channel_shorts.append(vid_id)
                    video_to_channel[vid_id] = channel_title

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if channel_shorts:
            with discovery_logs_lock:
                discovery_logs.append(f"Channel {idx}: Found {len(channel_shorts)} Shorts in '{channel_title}'")
            today_shorts.extend(channel_shorts)
        else:
            with discovery_logs_lock:
                discovery_logs.append(f"Channel {idx}: No Shorts found today in '{channel_title}'")

    # 2. If no Shorts found across all channels
    if not today_shorts:
        no_shorts_flag = True
        return

    # 3. Initialize data storage for discovered Shorts
    with data_lock:
        for vid in today_shorts:
            shorts_data.setdefault(vid, [])

    # 4. Immediate stats fetch (so UI can show something right away)
    now_ts = datetime.utcnow().isoformat() + "Z"
    for i in range(0, len(today_shorts), 50):
        batch_ids = today_shorts[i: i + 50]
        try:
            stats_resp = youtube.videos().list(part="statistics", id=",".join(batch_ids)).execute()
        except HttpError as e:
            error_message = f"YouTube API Error (initial stats fetch): {e}"
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

    # 5. Poll every hour
    while True:
        now_ts = datetime.utcnow().isoformat() + "Z"
        for i in range(0, len(today_shorts), 50):
            batch_ids = today_shorts[i: i + 50]
            try:
                stats_resp = youtube.videos().list(part="statistics", id=",".join(batch_ids)).execute()
            except HttpError as e:
                error_message = f"YouTube API Error (polling stats): {e}"
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
    """Starts the polling thread once per Streamlit session."""
    thread = threading.Thread(target=poll_stats_hourly, daemon=True)
    thread.start()
    return thread


# ----------------------------- Main UI -------------------------------

# Start the background thread
start_background_thread()

# Header
st.title("📊 YouTube Shorts VPH & Engagement Tracker")

# Show discovery logs in real-time
st.subheader("Discovery Progress")
with discovery_logs_lock:
    for log in discovery_logs:
        st.write(log)

# Display API error if it occurred
if error_message:
    st.error(error_message)
    st.stop()

# If discovery finished with no Shorts
if no_shorts_flag:
    st.info("No Shorts (≤ 3 minutes) were uploaded today for the selected channels.")
    st.stop()

# Otherwise, if still discovering or initial stats fetch not done
with data_lock:
    all_videos = list(shorts_data.keys())

if not all_videos:
    st.info("Waiting for initial stats fetch to complete...")
    st.stop()

# Once data is available, show the dropdown with channel names and video IDs
st.subheader("Available Shorts")
select_options = [f"{video_to_channel[vid]} → {vid}" for vid in all_videos]
selected_option = st.selectbox("Select a channel → video ID:", select_options)
selected_vid = selected_option.split(" → ")[1]

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

# Show the selected video's channel name above metrics
video_channel = video_to_channel.get(selected_vid, "Unknown Channel")
st.subheader(f"Metrics for: {video_channel} → {selected_vid}")

latest = df.iloc[-1]
st.markdown(f"""
- **Timestamp (UTC):** {latest['timestamp']}
- **Total Views:** {int(latest['viewCount'])}
- **Views Per Hour (VPH):** {int(latest['vph'])}
- **Approximate Engagement Rate:** {latest['engagement_rate']:.2%}
""")

st.subheader("VPH Over Time")
st.line_chart(df.set_index("timestamp")["vph"])

st.subheader("Engagement Rate Over Time")
st.line_chart(df.set_index("timestamp")["engagement_rate"])

st.subheader("Raw Data Table")
st.dataframe(df.reset_index(drop=True), use_container_width=True)
