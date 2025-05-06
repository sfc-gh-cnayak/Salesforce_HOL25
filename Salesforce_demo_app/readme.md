# 🏝️ Resort Cortex AI Assistant

A Snowflake Native App that leverages Cortex Analyst, Streamlit, and Marketplace data to provide a conversational assistant for resort operations.

---

## 📦 What This App Does

The Resort Cortex AI Assistant provides real-time insights and analytics on:

- 🎥 **Vision Inference Data**  
  Detects valuables like passports, purses, wallets, and phones across resort zones using object detection.

- 🧳 **Lost & Found Reports**  
  Matches guest-reported lost items with real-time camera detections.

- 🧑‍💼 **Guest Check-in Metadata**  
  Associates guests with object detections based on check-in time and room location.

- 🌦️ **Weather Conditions**  
  Adds weather data from the Snowflake Marketplace to understand environmental context.

- 🤖 **Natural Language Analytics**  
  Uses **Cortex Analyst** to answer business questions like:
  > “Were any valuable items detected near the Main Pool during the rain yesterday?”

---

## 🧠 Semantic Model (Cortex Analyst)

Built on top of the unified `resort_event_unified_table`, the semantic model includes:

- **Dimensions:** object type, guest tier, zone type, room number, weather condition, time
- **Measures:** count of items, count of valuables, match rate with lost reports, average confidence score

---

## 🖥️ Streamlit UI

The native app includes a Streamlit-based homepage for:
- Project overview
- Use case exploration
- Contextual descriptions of the data model

More interactive dashboards and chatbot views can be added in future pages.



## 🚀 How to Use

### Setup via Snow CLI

```bash
snow app run
