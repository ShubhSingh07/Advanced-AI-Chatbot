"""
Fixed app.py with proper imports and error handling
"""
import sys
import os
from pathlib import Path

# Fix imports by adding project root to path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(current_dir))

# Suppress SSL warnings (optional)
import warnings
warnings.filterwarnings('ignore', message='urllib3 v2 only supports OpenSSL')

import gradio as gr

# Try to import Utils, create dummy functions if they don't exist
try:
    from Utils.upload_file import upload_file_and_get_embeddings
except ImportError:
    print("Warning: Utils.upload_file not found, using dummy function")
    def upload_file_and_get_embeddings(files):
        if files:
            print(f"Files uploaded: {[f.name if hasattr(f, 'name') else f for f in files]}")
            return "Files processed successfully"
        return None

try:
    from Utils.chatbot import chatbot_response
except ImportError:
    print("Warning: Utils.chatbot not found, using dummy function")
    def chatbot_response(user_input):
        return f"Echo: {user_input}"

try:
    from Utils.ui_settings import get_ui_settings
except ImportError:
    print("Warning: Utils.ui_settings not found, using defaults")
    def get_ui_settings():
        return {}

# Define feedback handler
def feedback_handler(data: gr.LikeData):
    """Handle user feedback on chatbot responses"""
    print(f"User feedback - Index: {data.index}, Liked: {data.liked}")
    # Add your feedback processing logic here
    pass

# Create Gradio interface
with gr.Blocks(title="QnA and RAG with SQL") as demo:
    gr.Markdown("# QnA and RAG with SQL and Tabular Data")
    
    with gr.Tabs():
        with gr.TabItem("Chat"):
            with gr.Row():
                chatbot = gr.Chatbot(
                    [],
                    elem_id="chatbot",
                    bubble_full_width=False,
                    height=500,
                    avatar_images=(
                        (str(project_root / "images/AI_RT.png"), 
                         str(project_root / "images/openai.png"))
                    ) if (project_root / "images").exists() else None,
                    show_label=False
                )
            
            with gr.Row():
                with gr.Column(scale=8):
                    msg = gr.Textbox(
                        label="Your Question",
                        placeholder="Ask a question about your data...",
                        show_label=False
                    )
                with gr.Column(scale=1):
                    submit_btn = gr.Button("Submit", variant="primary")
            
            with gr.Row():
                clear_btn = gr.Button("Clear Chat")
                
        with gr.TabItem("Upload Data"):
            gr.Markdown("### Upload your data files")
            file_upload = gr.File(
                label="Upload Data Files (CSV, Excel, SQL Database, etc.)",
                file_count="multiple",
                file_types=[".csv", ".xlsx", ".xls", ".db", ".sql"]
            )
            upload_status = gr.Textbox(
                label="Upload Status",
                interactive=False
            )
    
    # Connect handlers
    def user_message(user_input, history):
        """Handle user message submission"""
        if not user_input.strip():
            return "", history
        return "", history + [[user_input, None]]
    
    def bot_response(history):
        """Generate bot response"""
        if not history or history[-1][1] is not None:
            return history
        
        user_input = history[-1][0]
        try:
            bot_message = chatbot_response(user_input)
        except Exception as e:
            bot_message = f"Error: {str(e)}"
        
        history[-1][1] = bot_message
        return history
    
    def clear_chat():
        """Clear chat history"""
        return []
    
    def handle_file_upload(files):
        """Handle file upload"""
        if not files:
            return "No files uploaded"
        
        try:
            result = upload_file_and_get_embeddings(files)
            if result:
                return f"Successfully processed {len(files)} file(s)"
            return "Files uploaded but processing failed"
        except Exception as e:
            return f"Error processing files: {str(e)}"
    
    # Connect events
    msg.submit(user_message, [msg, chatbot], [msg, chatbot]).then(
        bot_response, chatbot, chatbot
    )
    submit_btn.click(user_message, [msg, chatbot], [msg, chatbot]).then(
        bot_response, chatbot, chatbot
    )
    clear_btn.click(clear_chat, None, chatbot)
    
    # File upload handler
    file_upload.change(
        handle_file_upload,
        inputs=[file_upload],
        outputs=[upload_status]
    )
    
    # Feedback handler
    chatbot.like(feedback_handler)

if __name__ == "__main__":
    print("=" * 60)
    print("Starting Advanced AI Chatbot")
    print(f"Project root: {project_root}")
    print(f"Python path: {sys.path[:3]}")
    print("=" * 60)
    
    # Launch with error handling
    try:
        demo.launch(
            server_name="0.0.0.0",
            server_port=7860,
            share=False,  # Set to True for public URL
            debug=True,
            show_error=True,
            inbrowser=False
        )
    except Exception as e:
        print(f"Error launching app: {e}")
        print("\nTrying alternative configuration...")
        demo.launch(
            server_name="127.0.0.1",
            server_port=7860,
            share=True,  # Generate public URL as fallback
            debug=True
        )