"""Application entry point for the Gradio app."""

from app.utils.logger import configure_logging
from app.ui.gradio_app import main


if __name__ == "__main__":
    configure_logging()
    main()
