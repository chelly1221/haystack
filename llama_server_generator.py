from typing import AsyncGenerator, Optional, List, Dict, Any
import httpx
import json
import logging

logger = logging.getLogger(__name__)


class LlamaServerGenerator:
    """OpenAI-compatible vLLM server generator with streaming and tool use support"""
    
    def __init__(self, 
                 server_url: str = "http://192.168.10.101:8080",
                 default_max_tokens: int = 10000,
                 default_temperature: float = 0.1,
                 default_top_p: float = 0.9):
        """
        Initialize the LlamaServerGenerator
        
        Args:
            server_url: The vLLM server base URL
            default_max_tokens: Default maximum tokens to generate
            default_temperature: Default temperature for sampling
            default_top_p: Default top_p for nucleus sampling
        """
        # vLLM OpenAI 서버의 올바른 엔드포인트 사용
        self.base_url = server_url
        self.completion_url = f"{server_url}/v1/completions"
        self.chat_url = f"{server_url}/v1/chat/completions"
        
        # 기본 생성 파라미터 설정
        self.default_max_tokens = default_max_tokens
        self.default_temperature = default_temperature
        self.default_top_p = default_top_p
        
    async def stream(self, 
                     prompt: str,
                     use_chat_format: bool = False,
                     max_tokens: Optional[int] = None,
                     temperature: Optional[float] = None,
                     top_p: Optional[float] = None,
                     top_k: Optional[int] = None,
                     frequency_penalty: Optional[float] = None,
                     presence_penalty: Optional[float] = None,
                     repetition_penalty: Optional[float] = None,
                     stop: Optional[list] = None,
                     tools: Optional[List[Dict[str, Any]]] = None,
                     tool_choice: Optional[str] = None,
                     **kwargs) -> AsyncGenerator[str, None]:
        """
        Stream text generation from vLLM OpenAI-compatible server with tool use support
        
        Args:
            prompt: The input prompt
            use_chat_format: Whether to use chat completion format
            max_tokens: Maximum number of tokens to generate
            temperature: Sampling temperature
            top_p: Nucleus sampling parameter
            top_k: Top-k sampling parameter
            frequency_penalty: Frequency penalty
            presence_penalty: Presence penalty
            repetition_penalty: Repetition penalty
            stop: List of stop sequences
            tools: List of tool definitions for function calling
            tool_choice: How to choose tools ("auto", "none", or specific tool)
            **kwargs: Additional parameters
        
        Yields:
            str: Generated text chunks
        """
        
        # 기본값 적용
        generation_params = {
            "max_tokens": max_tokens if max_tokens is not None else self.default_max_tokens,
            "temperature": temperature if temperature is not None else self.default_temperature,
            "top_p": top_p if top_p is not None else self.default_top_p,
        }
        
        # 선택적 파라미터 추가 (값이 제공된 경우에만)
        if top_k is not None:
            generation_params["top_k"] = top_k
        if frequency_penalty is not None:
            generation_params["frequency_penalty"] = frequency_penalty
        if presence_penalty is not None:
            generation_params["presence_penalty"] = presence_penalty
        if repetition_penalty is not None:
            generation_params["repetition_penalty"] = repetition_penalty
        if stop is not None:
            generation_params["stop"] = stop
            
        # Tool use parameters
        if tools is not None:
            generation_params["tools"] = tools
            if tool_choice is not None:
                generation_params["tool_choice"] = tool_choice
                
        # 추가 kwargs 병합
        generation_params.update(kwargs)
        
        if use_chat_format or tools is not None:
            # Chat completion format (required for tool use)
            # Convert prompt to chat format if needed
            if isinstance(prompt, str):
                # Parse the prompt to extract system and user messages
                messages = self._parse_prompt_to_messages(prompt)
            else:
                messages = prompt
                
            payload = {
                "model": "/models/A.X-3.1-Light",  # 모델 경로
                "messages": messages,
                "stream": True,
                **generation_params
            }
            url = self.chat_url
        else:
            # Text completion format
            payload = {
                "model": "/models/A.X-3.1-Light",  # 모델 경로
                "prompt": prompt,
                "stream": True,
                **generation_params
            }
            url = self.completion_url
        
        # 로깅 추가 (디버깅용)
        logger.info(f"🚀 Streaming from: {url}")
        logger.info(f"📊 Generation parameters: max_tokens={generation_params.get('max_tokens')}, "
                   f"temperature={generation_params.get('temperature')}, "
                   f"top_p={generation_params.get('top_p')}")
        if tools:
            logger.info(f"🛠️ Tool use enabled with {len(tools)} tools")
        
        timeout = httpx.Timeout(timeout=600.0, read=600.0, connect=300.0)
        
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("POST", url, json=payload) as response:
                    response.raise_for_status()
                    
                    tool_calls_buffer = []
                    current_tool_call = None
                    
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                            
                        if line.startswith("data: "):
                            data_str = line[6:]  # Remove "data: " prefix
                            
                            if data_str == "[DONE]":
                                break
                            
                            try:
                                data = json.loads(data_str)
                                
                                # Extract text based on format
                                if use_chat_format or tools is not None:
                                    # Chat completion format with potential tool calls
                                    if "choices" in data and len(data["choices"]) > 0:
                                        choice = data["choices"][0]
                                        
                                        # Check for tool calls
                                        if "delta" in choice:
                                            delta = choice["delta"]
                                            
                                            # Handle tool calls
                                            if "tool_calls" in delta:
                                                for tool_call in delta["tool_calls"]:
                                                    if "function" in tool_call:
                                                        # Process tool call
                                                        function = tool_call["function"]
                                                        if "name" in function:
                                                            current_tool_call = {
                                                                "name": function["name"],
                                                                "arguments": ""
                                                            }
                                                        if "arguments" in function and current_tool_call:
                                                            current_tool_call["arguments"] += function["arguments"]
                                                        
                                                        # Check if tool call is complete
                                                        if current_tool_call and self._is_json_complete(current_tool_call["arguments"]):
                                                            tool_calls_buffer.append(current_tool_call)
                                                            # Process the tool call (for HTML preservation)
                                                            result = self._process_tool_call(current_tool_call)
                                                            if result:
                                                                yield result
                                                            current_tool_call = None
                                            
                                            # Handle regular content
                                            content = delta.get("content", "")
                                            if content:
                                                yield content
                                else:
                                    # Text completion format
                                    if "choices" in data and len(data["choices"]) > 0:
                                        text = data["choices"][0].get("text", "")
                                        if text:
                                            yield text
                                            
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON: {data_str}")
                                continue
                                
        except httpx.HTTPStatusError as e:
            # 스트리밍 응답에서는 response.text를 직접 읽을 수 없음
            error_msg = f"HTTP {e.response.status_code}"
            logger.error(f"❌ HTTP error: {error_msg}")
            
            # 에러 응답 본문을 안전하게 읽기
            try:
                if hasattr(e.response, 'read'):
                    error_body = await e.response.read()
                    error_text = error_body.decode('utf-8', errors='ignore')
                    logger.error(f"Error details: {error_text}")
                    yield f"Error: {error_msg} - {error_text}"
                else:
                    yield f"Error: {error_msg}"
            except:
                yield f"Error: {error_msg}"
                
        except httpx.TimeoutException:
            logger.error("❌ Request timeout")
            yield "Error: Request timeout"
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            yield f"Error: {str(e)}"
    
    def _parse_prompt_to_messages(self, prompt: str) -> List[Dict[str, str]]:
        """Parse a formatted prompt string into chat messages"""
        messages = []
        
        # Try to parse the prompt if it's in the special format
        if "<|im_start|>" in prompt:
            parts = prompt.split("<|im_start|>")
            for part in parts[1:]:  # Skip the first empty part
                if "<|im_end|>" in part:
                    role_content = part.split("<|im_end|>")[0].strip()
                    if role_content.startswith("system"):
                        role = "system"
                        content = role_content[6:].strip()
                    elif role_content.startswith("user"):
                        role = "user"
                        content = role_content[4:].strip()
                    elif role_content.startswith("assistant"):
                        role = "assistant"
                        content = role_content[9:].strip()
                    else:
                        continue
                    
                    if content:
                        messages.append({"role": role, "content": content})
        else:
            # Default to user message
            messages = [{"role": "user", "content": prompt}]
        
        return messages
    
    def _is_json_complete(self, json_str: str) -> bool:
        """Check if a JSON string is complete"""
        try:
            json.loads(json_str)
            return True
        except:
            return False
    
    def _process_tool_call(self, tool_call: Dict[str, Any]) -> Optional[str]:
        """Process a tool call and return any additional content"""
        try:
            # Parse the tool call arguments
            if tool_call["name"] == "process_html_content":
                args = json.loads(tool_call["arguments"])
                if args.get("include_images", False):
                    # Return a signal that images should be preserved
                    return ""  # The actual image handling is done in the query router
        except:
            pass
        return None
    
    async def __call__(self, 
                       prompt: str, 
                       max_tokens: Optional[int] = None,
                       **kwargs) -> AsyncGenerator[str, None]:
        """Allow calling the instance directly for streaming"""
        async for chunk in self.stream(prompt, max_tokens=max_tokens, **kwargs):
            yield chunk


# Convenience function for quick streaming
async def stream_llama(prompt: str, 
                      server_url: str = "http://192.168.10.101:8080",
                      use_chat_format: bool = False,
                      max_tokens: int = 2048,
                      temperature: float = 0.7,
                      top_p: float = 0.95,
                      tools: Optional[List[Dict[str, Any]]] = None,
                      tool_choice: Optional[str] = None,
                      **kwargs) -> AsyncGenerator[str, None]:
    """
    Quick streaming function without instantiating the class
    
    Args:
        prompt: The input prompt
        server_url: The vLLM server base URL
        use_chat_format: Whether to use chat completion format
        max_tokens: Maximum number of tokens to generate
        temperature: Sampling temperature
        top_p: Nucleus sampling parameter
        tools: List of tool definitions for function calling
        tool_choice: How to choose tools
        **kwargs: Additional generation parameters
    
    Yields:
        str: Generated text chunks
    """
    generator = LlamaServerGenerator(
        server_url,
        default_max_tokens=max_tokens,
        default_temperature=temperature,
        default_top_p=top_p
    )
    async for chunk in generator.stream(prompt, 
                                       use_chat_format=use_chat_format, 
                                       max_tokens=max_tokens,
                                       temperature=temperature,
                                       top_p=top_p,
                                       tools=tools,
                                       tool_choice=tool_choice,
                                       **kwargs):
        yield chunk