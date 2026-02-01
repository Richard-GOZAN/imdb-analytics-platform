"""
IMDB Chat Interface - Final Version

Streamlit application featuring:
- Natural language to SQL query generation
- Usage statistics tracking (questions, tokens, session time)
- Model selection (gpt-4o, gpt-4o-mini, gpt-4-turbo)
- CSV export functionality
- 10 curated example questions in sidebar
- Clean and intuitive UI

Run with:
    streamlit run app/chat.py
"""

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

from app.agent import run_agent
from app.stats import ConversationStats

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="IMDB Chat",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
.stMetric {
    background-color: #0e1117;
    padding: 10px;
    border-radius: 5px;
}
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "stats" not in st.session_state:
    st.session_state.stats = ConversationStats()

if "selected_model" not in st.session_state:
    st.session_state.selected_model = "gpt-4o"

# Sidebar Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Model Selection (simplified)
    model = st.selectbox(
        "🤖 Model",
        ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
        index=1,  # Default to gpt-4o-mini (cheaper)
        help="gpt-4o: Best quality\ngpt-4o-mini: Faster & cheaper\ngpt-4-turbo: Previous generation"
    )
    st.session_state.selected_model = model
    
    st.divider()
    
    # Statistics (3 core metrics)
    st.header("📊 Statistics")
    
    stats_summary = st.session_state.stats.get_summary()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Questions", stats_summary["total_questions"])
    col2.metric("Tokens", st.session_state.stats.get_formatted_tokens())
    col3.metric("Session", f"{stats_summary['session_duration_minutes']:.0f}m")
    
    # Detailed stats expander
    with st.expander("📈 Detailed Stats"):
        st.write(f"**Input tokens:** {stats_summary['input_tokens']:,}")
        st.write(f"**Output tokens:** {stats_summary['output_tokens']:,}")
        
        if stats_summary['model_usage']:
            st.write("**Model usage:**")
            for model_name, count in stats_summary['model_usage'].items():
                st.write(f"  - {model_name}: {count} queries")
    
    st.divider()
    
    # Example Questions
    st.header("💡 Example Questions")
    
    examples = [
        "Top 10 highest-rated movies",
        "Christopher Nolan's filmography",
        "Actors who worked with Scorsese",
        "Best movies from the 1990s",
        "Tom Hanks + Spielberg collaborations",
        "Most prolific directors",
        "Highest-rated sci-fi movies",
        "Leonardo DiCaprio's best films",
        "Movies with 9+ rating from 2010s",
        "Directors with most movies",
    ]
    
    for example in examples:
        if st.button(example, key=f"example_{example}", use_container_width=True):
            # Add user message
            st.session_state.messages.append({
                "role": "user",
                "content": example,
            })
            
            # Process immediately
            with st.spinner("Thinking..."):
                result = run_agent(
                    user_question=example,
                    conversation_history=[
                        {"role": msg["role"], "content": msg["content"]}
                        for msg in st.session_state.messages[:-1]
                    ],
                    model=st.session_state.selected_model,
                )
                
                # Track statistics
                if "usage" in result and result["usage"]:
                    st.session_state.stats.add_query(
                        model=st.session_state.selected_model,
                        input_tokens=result["usage"].get("prompt_tokens", 0),
                        output_tokens=result["usage"].get("completion_tokens", 0),
                        question=example,
                        sql_executed=result["sql_query"] is not None,
                    )
                
                # Add assistant response
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result["answer"],
                    "sql": result["sql_query"],
                    "data": result["data"],
                })
            
            st.rerun()
    
    st.divider()
    
    # Clear conversation button
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.stats = ConversationStats()
        st.rerun()
    
    # Info Footer
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        st.caption("🤖 Powered by OpenAI")
        st.caption(f"📊 Model: {st.session_state.selected_model}")
    with col2:
        st.caption("🎬 IMDB Dataset")
        st.caption("📅 Updated: January 2026")

# Main Chat Area
st.title("🎬 IMDB Movie Database Chat")
st.markdown(
    "Ask me anything about movies, actors, and directors! "
    "I'll query the IMDB database and provide intelligent answers."
)

# Display conversation history
for idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display SQL if available
        if "sql" in message and message["sql"]:
            with st.expander("📝 View SQL Query"):
                st.code(message["sql"], language="sql")
        
        # Display data table if available
        if "data" in message and message["data"] is not None and not message["data"].empty:
            # Show result count
            st.caption(f"📊 {len(message['data'])} results")
            st.dataframe(message["data"], use_container_width=True)
            
            # Export button
            csv = message["data"].to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"imdb_results_{idx}.csv",
                mime="text/csv",
                key=f"download_{idx}",
            )
        

# Chat input
if prompt := st.chat_input("Ask about movies, actors, or directors..."):
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": prompt,
    })
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Display assistant response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            # Run agent with selected model
            result = run_agent(
                user_question=prompt,
                conversation_history=[
                    {"role": msg["role"], "content": msg["content"]}
                    for msg in st.session_state.messages[:-1]
                ],
                model=st.session_state.selected_model,
            )
            
            # Track statistics
            if "usage" in result and result["usage"]:
                st.session_state.stats.add_query(
                    model=st.session_state.selected_model,
                    input_tokens=result["usage"].get("prompt_tokens", 0),
                    output_tokens=result["usage"].get("completion_tokens", 0),
                    question=prompt,
                    sql_executed=result["sql_query"] is not None,
                )
            
            # Display answer
            st.markdown(result["answer"])
            
            # Display SQL if available
            if result["sql_query"]:
                with st.expander("📝 View SQL Query"):
                    st.code(result["sql_query"], language="sql")
            
            # Display results table
            if result["data"] is not None and not result["data"].empty:
                # Show result count
                st.caption(f"📊 {len(result['data'])} results")
                st.dataframe(result["data"], use_container_width=True)
                
                # Export button
                csv = result["data"].to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="imdb_results.csv",
                    mime="text/csv",
                )
            
            # Add to conversation history
            st.session_state.messages.append({
                "role": "assistant",
                "content": result["answer"],
                "sql": result["sql_query"],
                "data": result["data"],
            })