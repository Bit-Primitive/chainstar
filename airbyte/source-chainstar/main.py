"""Entry point of the chainstar source connector: `python main.py spec|check|discover|read ...`.

Needs chainstar with the `airbyte` extra in the same environment. The Docker image
runs the same thing as `python -m chainstar.integrations.airbyte`.
"""

from chainstar.integrations.airbyte import run

if __name__ == "__main__":
    run()
