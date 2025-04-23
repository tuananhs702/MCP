import asyncio
import sys
from typing import Optional
from contextlib import AsyncExitStack
import os
from dotenv import load_dotenv
import json
import pprint
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from openai import OpenAI  # OpenRouter-compatible

load_dotenv()

class MCPClient:
    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.llm = OpenAI(
            api_key=os.getenv("OPENROUTER_API_KEY") or "sk-your-key-here",
            base_url="https://openrouter.ai/api/v1"
        )
        self.model = "anthropic/claude-3.5-sonnet"  # Model miễn phí (GLM Z1 32B)

    async def connect_to_server(self, server_script_path: str):
        if not server_script_path.endswith('.py'):
            raise ValueError("Only Python server scripts are currently supported.")
        
        server_params = StdioServerParameters(
            command="python",
            args=[server_script_path],
            env=None
        )

        stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        self.stdio, self.write = stdio_transport
        self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))
        await self.session.initialize()

        response = await self.session.list_tools()
        tools = response.tools
        print("\n✅ Connected to server with tools:", [tool.name for tool in tools])

    async def process_query(self, query: str) -> str:
        messages = [{"role": "user", "content": query}]
        final_text = []

        try:
            tool_response = await self.session.list_tools()
            available_tools = [{
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema
                }
            } for tool in tool_response.tools]

            # print(f"\n🛠 Available Tools: {[t['function']['name'] for t in available_tools]}")

            response = self.llm.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=available_tools,
                tool_choice="auto",
                max_tokens=128
            )

            # print("\n📦 RAW LLM RESPONSE:")
            # if hasattr(response, "model_dump"):
            #     pprint.pprint(response.model_dump(), indent=2)
            # else:
            #     pprint.pprint(response, indent=2)

            if not response or not hasattr(response, "choices") or not response.choices:
                return "❌ LLM không trả về kết quả hợp lệ (response.choices bị thiếu)."

            choice = response.choices[0]
            message = choice.message

            if hasattr(message, "tool_calls") and message.tool_calls:
                for tool_call in message.tool_calls:
                    try:
                        tool_name = tool_call.function.name
                        tool_args = json.loads(tool_call.function.arguments or "{}")

                        print(f"\n🔧 Gọi tool: {tool_name}({tool_args})")

                        result = await self.session.call_tool(tool_name, tool_args)

                        final_text.append(f"[→ Tool: {tool_name}({tool_args})]")
                        messages.append({"role": "user", "content": result.content})

                        followup = self.llm.chat.completions.create(
                            model=self.model,
                            messages=messages,
                            max_tokens=128
                        )

                        if followup and followup.choices:
                            final_text.append(followup.choices[0].message.content)
                        else:
                            final_text.append("⚠️ Model không phản hồi tiếp theo.")
                    except Exception as tool_err:
                        final_text.append(f"❌ Lỗi khi gọi tool `{tool_name}`: {tool_err}")
            else:
                final_text.append(message.content or "⚠️ Không có nội dung từ model.")

        except Exception as e:
            final_text.append(f"❌ Lỗi khi gọi LLM/OpenRouter: {str(e)}")

        return "\n".join(final_text)

    async def chat_loop(self):
        print("\n🧠 MCP Client Started! Type your queries or 'quit' to exit.")
        while True:
            try:
                query = input("\nQuery: ").strip()
                if query.lower() == 'quit':
                    break
                response = await self.process_query(query)
                print("\n" + response)
            except Exception as e:
                print(f"\n❌ Error: {str(e)}")

    async def cleanup(self):
        await self.exit_stack.aclose()

async def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_server_script>")
        sys.exit(1)
    client = MCPClient()
    try:
        await client.connect_to_server(sys.argv[1])
        await client.chat_loop()
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
