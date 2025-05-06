# QuantPulse Agent

from typing import Dict, List, Optional

import _snowflake
import json
import streamlit as st
import time
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permissions

sp_session = get_active_session()

DATABASE = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
SCHEMA = "CORE"
STAGE = "LIB_STG"
FILE = "salesforce_lai_agent.yaml"
st.set_page_config(
    page_title="Resort Cortex AI Assistant",
    layout="wide"
)

st.session_state.setdefault("imported_privilege_granted", False)

def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }

    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )

    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        st.session_state.messages.pop()
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            request_id = response["request_id"]
            content = response["message"]["content"]
            st.session_state.messages.append(
                {**response['message'], "request_id": request_id}
            )
            display_content(content=content, request_id=request_id)  # type: ignore[arg-type]


def display_content(
    content: List[Dict[str, str]],
    request_id: Optional[str] = None,
    message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql(item["statement"])


@st.cache_data
def display_sql(sql: str) -> None:
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            session = get_active_session()
            df = session.sql(sql).to_pandas()
            if len(df.index) > 1:
                data_tab, line_tab, bar_tab = st.tabs(
                    ["Data", "Line Chart", "Bar Chart"]
                )
                data_tab.dataframe(df)
                if len(df.columns) > 1:
                    df = df.set_index(df.columns[0])
                try:
                    with line_tab:
                        st.line_chart(df)
                    with bar_tab:
                        st.bar_chart(df)
                except Exception as e:
                    # st.info("Multiple values so couldn't plot the graphs")
                    pass
            else:
                st.dataframe(df)


def show_conversation_history() -> None:
    for message_index, message in enumerate(st.session_state.messages):
        chat_role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(chat_role):
            display_content(
                content=message["content"],
                request_id=message.get("request_id"),
                message_index=message_index,
            )


def reset() -> None:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None


app_db = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
app_sch = 'CRM'
st.title("Resort Agent")
st.markdown(f"Semantic Model: `{FILE}`")



with st.expander("💡 Sample Questions you can ask:", expanded=True):
    st.markdown("""
- What valuables were detected near the Main Pool last week?  
- How many phones were reported lost and also detected by the cameras?  
- Which zones have the highest number of valuable item detections?  
- How did weather conditions like rain or wind affect valuable item detections?
    """)

with st.expander("💬 More questions that the Agent can answer", expanded=False):
    st.markdown("### 🧳 Lost & Found Matching")
    st.markdown("""
- Were any phones detected and also reported as lost?  
- What’s the time gap between item detection and the matching lost report?  
- Which object types are most often lost by guests and found later via camera?  
- How many detected items were matched to lost reports within 12 hours?  
- Show a list of all detected items that were also reported lost during rainy days.
    """)

    st.markdown("### 📍 Zone & Camera Monitoring")
    st.markdown("""
- Which camera zones detect the most valuable items?  
- Show detections in indoor zones with high item value.  
- Which locations have the most unattended laptops or electronics?  
- What’s the average detection confidence by camera or floor?  
- Compare detection trends across public vs restricted areas.
    """)

    st.markdown("### 🌦️ Weather-Linked Trends")
    st.markdown("""
- How does rainy weather affect the number of valuables detected?  
- Were more items lost on windy days than on clear ones?  
- What’s the trend in object detections on hot vs cold days?  
- Show valuables detected during stormy weather near outdoor zones.  
- Do valuables go unreported more during bad weather?
    """)

    st.markdown("### 🧑 Guest Behavior & Loyalty Insights")
    st.markdown("""
- Do VIP guests lose more valuables than regular guests?  
- Which loyalty tier is most associated with detected valuable items?  
- What’s the average number of valuables linked to solo vs family travelers?  
- Did any Gold-tier guests lose items last week?  
- Show valuables detected near rooms assigned to VIPs.
    """)

    st.markdown("### ⏱️ Time-Based Trends")
    st.markdown("""
- What is the peak time of day for valuable item detections?  
- Which day of the week sees the most lost items?  
- Show hourly trends of valuable detections across 90 days.  
- Were more valuables detected during weekends or weekdays?  
- Show a time series of daily valuables detected by object type.
    """)

    st.markdown("### 🚨 Proactive Risk Monitoring")
    st.markdown("""
- What valuable items were detected in restricted zones during the last storm?  
- Show high-confidence detections of electronics in unauthorized areas.  
- Which zones consistently detect unattended passports or wallets?  
- Did any detections occur near guest-only zones with no associated check-in?  
- Are there any repeat object detections in the same area?
    """)



sp_session.sql("call {0}.PYTHON_FUNCTIONS.sp_init('{0}');".format(app_db)).collect()

if not st.session_state.imported_privilege_granted:
    held_privs = permissions.get_held_account_privileges(["IMPORTED PRIVILEGES ON SNOWFLAKE DB"])

    if held_privs:
        st.session_state.imported_privilege_granted = True
    else:
        permissions.request_account_privileges(["IMPORTED PRIVILEGES ON SNOWFLAKE DB"])
        st.warning("Please grant 'IMPORTED PRIVILEGES ON SNOWFLAKE DB' to use Cortex Agent.")
        

if "messages" not in st.session_state:
    reset()

with st.sidebar:
    if st.button("Reset conversation"):
        reset()

show_conversation_history()

if user_input := st.chat_input("How can I help?"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None