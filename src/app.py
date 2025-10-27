import gradio as gr
from Utils.upload_file import upload_file_and_get_embeddings
from Utils.chatbot import chatbot_response
from Utils.ui_settings import get_ui_settings


with gr.Blocks() as demo:
    with gr.Tabs():
        with gr.TabItem("QnA-and-RAG-with-SQL-and-Tablar-Data"):
             with gr.Row() as row_one:
                 chatbot = gr.Chatbot(
                     [],
                     elem_id="chatbot",
                     bubble_full_width=False,
                     height=500,
                     avatar_images=(
                         ("images/AI_RT.png", "images/openai.png")
                     )
                 )

                 chatbot.like(UISettings.feedback, None, None)
                 