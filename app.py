import streamlit as st
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
from streamlit_autorefresh import st_autorefresh

# --------------------------- Configuration ---------------------------

# Must be the first Streamlit command in your script:
st.set_page_config(layout="wide")

# Auto-refresh every 2 seconds so logs and data updates appear in real time
st_autorefresh(interval=2000, limit=None)

# 1. YouTube API Key from Streamlit Secrets
API_KEY = st.secrets["youtube"]["api_key"]

# 2. List of Channel IDs to Track
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

# Initialize session state if not already
if "discovered" not in st.session_state:
    st.session_state.discovered = False
    st.session_state.shorts_data = {}           # {video_id: [(timestamp, viewCount, likeCount, commentCount), ...]}
    st.session_state.video_to_channel = {}       # {video_id: channel_title}
    st.session_state.logs = []                   # Discovery logs
    st.session_state.no_shorts = False
    st.session_state.error = None

# ----------------------- Helper Functions ----------------------------

@st.cache_resource
def get_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)


def iso8601_to_seconds(duration_str: str) -> int:
    try:
        duration = parse_duration(duration_str)
        return int(duration.total_seconds())
    except:
        return 0


def get_midnight_ist_utc() -> datetime:
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    today_ist_date = now_ist.date()
    midnight_ist = datetime(today_ist_date.year, today_ist_date.month, today_ist_date.day, 0, 0)
    return midnight_ist - timedelta(hours=5, minutes=30)


def is_within_today(published_at_str: str) -> bool:
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight = midnight_utc + timedelta(hours=24)
    return midnight_utc <= pub_dt < next_midnight


def run_discovery_and_initial_fetch():
    youtube = get_youtube_client()
    today_shorts = []

    for idx, channel_id in enumerate(CHANNEL_IDS, start=1):
        try:
            ch_resp = youtube.channels().list(part="snippet,contentDetails", id=channel_id).execute()
            channel_title = ch_resp["items"][0]["snippet"]["title"]
            uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        except HttpError as e:
            st.session_state.error = f"API Error (channel fetch): {e}"
            return

        st.session_state.logs.append(f"Checking channel {idx}/{len(CHANNEL_IDS)}: {channel_title}")
        channel_shorts = []

        # Page through the uploads playlist
        pl_req = youtube.playlistItems().list(part="snippet", playlistId=uploads_playlist, maxResults=50)
        while pl_req:
            try:
                pl_resp = pl_req.execute()
            except HttpError as e:
                st.session_state.error = f"API Error (playlistItems): {e}"
                return

            for item in pl_resp["items"]:
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]
                if not is_within_today(published_at):
                    continue
                try:
                    cd_resp = youtube.videos().list(part="contentDetails", id=vid_id).execute()
                except HttpError as e:
                    st.session_state.error = f"API Error (contentDetails): {e}"
                    return
                duration_secs = iso8601_to_seconds(cd_resp["items"][0]["contentDetails"]["duration"])
                if duration_secs <= 180:
                    channel_shorts.append(vid_id)
                    st.session_state.video_to_channel[vid_id] = channel_title

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if channel_shorts:
            st.session_state.logs.append(f"Channel {idx}: Found {len(channel_shorts)} Shorts in '{channel_title}'")
            today_shorts.extend(channel_shorts)
        else:
            st.session_state.logs.append(f"Channel {idx}: No Shorts found today in '{channel_title}'")

    if not today_shorts:
        st.session_state.no_shorts = True
        return

    # Initialize data structure
    for vid in today_shorts:
        st.session_state.shorts_data.setdefault(vid, [])

    # Initial stats fetch
    now_ts = datetime.utcnow().isoformat() + "Z"
    for i in range(0, len(today_shorts), 50):
        batch = today_shorts[i : i + 50]
        try:
            stats_resp = youtube.videos().list(part="statistics", id=",".join(batch)).execute()
        except HttpError as e:
            st.session_state.error = f"API Error (initial stats): {e}"
            return
        for item in stats_resp["items"]:
            stats = item["statistics"]
            row = (
                now_ts,
                int(stats.get("viewCount", 0)),
                int(stats.get("likeCount", 0)),
                int(stats.get("commentCount", 0)),
            )
            st.session_state.shorts_data[item["id"]].append(row)

    st.session_state.discovered = True


def poll_stats_background():
    youtube = get_youtube_client()
    while True:
        if not st.session_state.discovered:
            time.sleep(5)
            continue
        vids = list(st.session_state.shorts_data.keys())
        now_ts = datetime.utcnow().isoformat() + "Z"
        for i in range(0, len(vids), 50):
            batch = vids[i : i + 50]
            try:
                stats_resp = youtube.videos().list(part="statistics", id=",".join(batch)).execute()
            except HttpError as e:
                st.session_state.error = f"API Error (polling): {e}"
                return
            for item in stats_resp["items"]:
                stats = item["statistics"]
                row = (
                    now_ts,
                    int(stats.get("viewCount", 0)),
                    int(stats.get("likeCount", 0)),
                    int(stats.get("commentCount", 0)),
                )
                st.session_state.shorts_data[item["id"]].append(row)
        # Sleep until next hour
        now = datetime.utcnow()
        secs = 3600 - (now.minute * 60 + now.second)
        time.sleep(secs)

# Start background polling thread once
if "poll_thread" not in st.session_state:
    t = threading.Thread(target=poll_stats_background, daemon=True)
    t.start()
    st.session_state.poll_thread = True

# Run discovery + initial fetch once
if not st.session_state.discovered and st.session_state.error is None:
    run_discovery_and_initial_fetch()

# ----------------------------- UI Rendering -----------------------------

st.title("📊 YouTube Shorts VPH & Engagement Tracker")

# Discovery logs
st.subheader("Discovery Progress")
for log in st.session_state.logs:
    st.write(log)

# Handle errors or no-shorts
if st.session_state.error:
    st.error(st.session_state.error)
    st.stop()

if st.session_state.no_shorts:
    st.info("No Shorts (≤ 3 minutes) were uploaded today for the selected channels.")
    st.stop()

if not st.session_state.discovered:
    st.info("Waiting for initial stats fetch to complete...")
    st.stop()

# Once data is available
vids = list(st.session_state.shorts_data.keys())
st.subheader("Available Shorts")
options = [f"{st.session_state.video_to_channel[v]} → {v}" for v in vids]
sel = st.selectbox("Select a channel → video", options)
selected_vid = sel.split(" → ")[1]

rows = st.session_state.shorts_data[selected_vid]
if not rows:
    st.warning("No stats captured yet for this video. Please wait a few minutes.")
    st.stop()

df = pd.DataFrame(rows, columns=["timestamp", "viewCount", "likeCount", "commentCount"])
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["vph"] = df["viewCount"].diff().fillna(0)
df["engagement_rate"] = (df["likeCount"] + df["commentCount"]) / df["viewCount"]

st.subheader(f"Metrics for: {st.session_state.video_to_channel[selected_vid]} → {selected_vid}")
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
st.dataframe(df, use_container_width=True)
