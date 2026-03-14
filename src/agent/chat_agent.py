"""
Chat agent using Claude API with tool_use for natural language analytics.

Usage:
    # As CLI
    python -m src.agent.chat_agent

    # As library
    from src.agent.chat_agent import ChatAgent
    agent = ChatAgent()
    response = agent.ask("What is our total revenue this year?")
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

import anthropic

from src.agent.tools.registry import get_tool_definitions, dispatch_tool
from src.agent.system_prompt import SYSTEM_PROMPT

# Import tools to trigger registration
import src.agent.tools.schema_tools
import src.agent.tools.query_tools
import src.agent.tools.analytics_tools
import src.agent.tools.visualization_tools
import src.agent.tools.cortex_tools
import src.agent.tools.report_tools


class ChatAgent:
    def __init__(self, model="claude-sonnet-4-20250514", max_turns=10):
        self.client = anthropic.Anthropic()
        self.model = model
        self.max_turns = max_turns
        self.messages = []
        self.tools = get_tool_definitions()

    def ask(self, question: str) -> dict:
        """Ask a question and get a response with tool use.

        Returns dict with 'text' (final answer) and 'tool_calls' (list of calls made).
        """
        self.messages.append({"role": "user", "content": question})
        tool_calls = []

        for _ in range(self.max_turns):
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=self.tools,
                messages=self.messages,
            )

            # Check if the model wants to use tools
            if response.stop_reason == "tool_use":
                # Add assistant message with tool use blocks
                self.messages.append({"role": "assistant", "content": response.content})

                # Process each tool use block
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = dispatch_tool(block.name, block.input)
                        tool_calls.append({
                            "tool": block.name,
                            "input": block.input,
                            "output": result,
                        })
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })

                self.messages.append({"role": "user", "content": tool_results})
            else:
                # Final text response
                text = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        text += block.text

                self.messages.append({"role": "assistant", "content": text})
                return {"text": text, "tool_calls": tool_calls}

        return {"text": "Reached maximum tool call iterations.", "tool_calls": tool_calls}

    def reset(self):
        """Clear conversation history."""
        self.messages = []


def main():
    """Interactive CLI chat loop."""
    agent = ChatAgent()
    print("Eyecare Analytics Agent (type 'quit' to exit, 'reset' to clear history)")
    print("=" * 60)

    while True:
        try:
            question = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not question:
            continue
        if question.lower() == "quit":
            break
        if question.lower() == "reset":
            agent.reset()
            print("Conversation reset.")
            continue

        print("\nThinking...")
        result = agent.ask(question)

        if result["tool_calls"]:
            print(f"\n[Used {len(result['tool_calls'])} tool(s): {', '.join(tc['tool'] for tc in result['tool_calls'])}]")

        print(f"\nAssistant: {result['text']}")


if __name__ == "__main__":
    main()
