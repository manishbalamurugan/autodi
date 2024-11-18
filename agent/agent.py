import os
import json
import openai
from dotenv import load_dotenv
from typing import List, Dict, Any
from .tools import GeoFileConverter

class Agent:
    def __init__(self):
        load_dotenv()
        
        self.system_prompt = """
        You are an advanced AI assistant specializing in geospatial data analysis and processing.
        You can process files in various formats (GeoJSON, CSV, Shapefile, GDB) and convert them 
        to standardized CSV formats with WGS84 coordinates.
        """
        
        self.messages = [{"role": "system", "content": self.system_prompt}]
        self.model = "gpt-4"
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.converter = GeoFileConverter()
        
        # Define available tools
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "convert_gdb_to_csv",
                    "description": "Convert a geodatabase file to CSV format",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "gdb_path": {"type": "string", "description": "Path to the GDB file"},
                            "output_folder": {"type": "string", "description": "Output directory path"}
                        },
                        "required": ["gdb_path", "output_folder"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "convert_shapefile_to_csv",
                    "description": "Convert a shapefile to CSV format",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "shp_path": {"type": "string", "description": "Path to the shapefile"},
                            "output_folder": {"type": "string", "description": "Output directory path"}
                        },
                        "required": ["shp_path", "output_folder"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "process_geojson",
                    "description": "Process GeoJSON file with optional geometry simplification",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "output_name": {"type": "string"},
                            "init_crs": {"type": "string"},
                            "simplify_tolerance": {"type": "number", "default": 0.001}
                        },
                        "required": ["file_path", "output_name", "init_crs"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "process_points",
                    "description": "Process point data from GeoJSON or CSV",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "file_path": {"type": "string"},
                            "output_name": {"type": "string"},
                            "init_crs": {"type": "string"}
                        },
                        "required": ["file_path", "output_name", "init_crs"]
                    }
                }
            }
        ]

    def ask(self, prompt: str, file_paths: Dict[str, str] = None) -> Dict[str, Any]:
        try:
            message_content = prompt
            if file_paths:
                message_content += f"\nAvailable files: {', '.join(file_paths.keys())}"

            self.messages.append({"role": "user", "content": message_content})
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                tools=self.tools
            )

            if response.choices[0].message.tool_calls:
                return self.execute_tool_calls(response.choices[0].message, file_paths)
            
            return {"response": response.choices[0].message.content}
            
        except Exception as e:
            return {"response": f"Error: {str(e)}"}

    def plan_and_execute(self, prompt: str, file_paths: Dict[str, str] = None, approve_plan: bool = False) -> Dict[str, Any]:
        try:
            planning_prompt = f"""
            Task: {prompt}
            
            Available files: {list(file_paths.keys()) if file_paths else 'None'}
            
            RESPOND ONLY WITH A JSON OBJECT IN THIS EXACT FORMAT:
            {{
                "explanation": "Brief explanation of what will be done",
                "function_calls": [
                    {{
                        "function": {{
                            "name": "name_of_function",
                            "arguments": {{
                                "arg1": "value1",
                                "arg2": "value2"
                            }}
                        }}
                    }}
                ]
            }}
            """

            self.messages.append({"role": "user", "content": planning_prompt})
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                functions=self.tools  # Use the functions parameter
            )

            # Parse the JSON response directly
            plan_dict = json.loads(response.choices[0].message.content)
            
            plan = {
                "requires_approval": True,
                "explanation": plan_dict["explanation"],
                "plan": {
                    "function_calls": plan_dict["function_calls"]
                }
            }
            
            if approve_plan:
                return self.execute_tool_calls(plan_dict["function_calls"], file_paths)
            
            return plan

        except Exception as e:
            return {
                "requires_approval": False,
                "explanation": f"Error creating plan: {str(e)}"
            }

    def execute_tool_calls(self, tool_calls: List[Dict], file_paths: Dict[str, str] = None) -> Dict[str, Any]:
        print(f"DEBUG: file_paths received: {file_paths}, {tool_calls}")
        if not tool_calls:
            return {"response": "No tool calls found."}

        try:
            results = []
            for tool_call in tool_calls:
                # Get the function details
                function_details = tool_call["function"]
                tool_name = function_details.get("name")
                arguments = json.loads(function_details.get("arguments", "{}"))

                # Replace filename with full path if it exists
                if file_paths and 'file_path' in arguments:
                    file_name = arguments['file_path']
                    if file_name in file_paths:
                        arguments['file_path'] = file_paths[file_name]
                    else:
                        return {"response": f"Error: File '{file_name}' not found."}

                # Execute the function
                try:
                    method = getattr(self.converter, tool_name)
                    result = method(**arguments)

                    if isinstance(result, dict):
                        results.append({
                            "response": f"Successfully executed {tool_name}",
                            "filename": result.get('filename'),
                            "file": result.get('response')
                        })
                    else:
                        results.append({
                            "response": f"Successfully executed {tool_name}"
                        })

                except Exception as e:
                    return {"response": f"Error executing {tool_name}: {str(e)}"}
                
            return results[-1] if results else {"response": "No results generated"}

        except Exception as e:
            return {"response": f"Error executing tool calls: {str(e)}"}