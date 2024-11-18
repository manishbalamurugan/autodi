import os
import requests
import json
import time
import threading
from typing import Dict

class GISAssistant:
    def __init__(self):
        self.api_url = "http://localhost:8000/api/v1"
        self.loaded_files = {}
        self.supported_formats = {
            '.csv': 'text/csv',
            '.geojson': 'application/geo+json',
            '.shp': 'application/x-shapefile',
            '.gdb': 'application/x-geodatabase'
        }
        self._stop_loading = False

    def _loading_animation(self, start_time: float) -> None:
        animation = "|/-\\"
        idx = 0
        while not self._stop_loading:
            elapsed = int(time.time() - start_time)
            print(f"\rProcessing {animation[idx]} ({elapsed}s)", end="", flush=True)
            idx = (idx + 1) % len(animation)
            time.sleep(0.1)

    def load_file(self, file_path: str) -> None:
        try:
            if not os.path.exists(file_path):
                print(f"Error: File not found - {file_path}")
                return

            file_name = os.path.basename(file_path)
            ext = os.path.splitext(file_path)[1].lower()
            
            if ext not in self.supported_formats:
                print(f"Error: Unsupported file format - {ext}")
                return

            self.loaded_files[file_name] = {
                "path": file_path,
                "type": self.supported_formats[ext]
            }
            print(f"Loaded file: {file_name}")
            
        except Exception as e:
            print(f"Error loading file: {str(e)}")

    def unload_file(self, filename: str) -> None:
        if filename in self.loaded_files:
            del self.loaded_files[filename]
            print(f"Unloaded file: {filename}")
        else:
            print(f"File {filename} not found")

    def clear_files(self) -> None:
        self.loaded_files.clear()
        print("All files unloaded")

    def list_files(self) -> None:
        if self.loaded_files:
            print("\nLoaded files:")
            for filename in self.loaded_files:
                print(f"- {filename}")
        else:
            print("No files loaded")

    def list_functions(self) -> None:
        try:
            response = requests.get(f"{self.api_url}/available_functions")
            functions = response.json()["functions"]
            print("\nAvailable Functions:")
            for func in functions:
                print(f"- {func}")
        except Exception as e:
            print(f"Error fetching functions: {str(e)}")

    def process_prompt(self, prompt: str) -> None:
        try:
            start_time = time.time()
            
            # Start loading animation
            self._stop_loading = False
            loading_thread = threading.Thread(target=self._loading_animation, args=(start_time,))
            loading_thread.daemon = True
            loading_thread.start()

            # Prepare files for upload
            files = []
            for name, info in self.loaded_files.items():
                files.append(
                    ("files", (name, open(info["path"], "rb"), info["type"]))
                )

            # Send initial request
            response = requests.post(
                f"{self.api_url}/process",
                data={"prompt": prompt, "approve_plan": "false"},
                files=files
            )

            # Stop loading animation
            self._stop_loading = True
            loading_thread.join()
            print("\r" + " " * 50 + "\r", end="", flush=True)

            result = response.json()

            if isinstance(result, dict) and "response" in result and not result.get("requires_approval"):
                print("\n(agent): ")
                print(result["response"])
                if "filename" in result:
                    print(f"\nOutput file: {result['filename']}")
                if "file" in result:
                    print("\nOutput preview:")
                    print(result["file"][:500] + "..." if len(result["file"]) > 500 else result["file"])
                return

            # Handle plan approval if needed
            if result.get("requires_approval"):
                print("\nProposed plan:")
                
                explanation = result.get("explanation")
                if explanation:
                    print(explanation)

                if "plan" in result and "function_calls" in result["plan"]:
                    for i, call in enumerate(result["plan"]["function_calls"], 1):
                        func = call["function"]
                        print(f"\nStep {i}:")
                        print(f" Function: {func['name']}")
                        print(" Arguments:")
                        for arg, value in json.loads(func['arguments']).items():
                            print(f" - {arg}: {value}")

                if input("\nApprove plan? (y/n): ").strip().lower() == 'y':
                    print("\nExecuting plan...")
                    
                    # Start loading animation for execution
                    self._stop_loading = False
                    loading_thread = threading.Thread(target=self._loading_animation, args=(time.time(),))
                    loading_thread.daemon = True
                    loading_thread.start()

                    # Execute approved plan
                    files = [
                        ("files", (name, open(info["path"], "rb"), info["type"]))
                        for name, info in self.loaded_files.items()
                    ]
                    
                    response = requests.post(
                        f"{self.api_url}/process",
                        data={
                            "prompt": prompt,
                            "approve_plan": "true",
                            "plan": json.dumps(result["plan"])
                        },
                        files=files
                    )

                    result = response.json()

                    # Stop loading animation
                    self._stop_loading = True
                    loading_thread.join()
                    print("\r" + " " * 50 + "\r", end="", flush=True)

                    # Handle execution result
                    if isinstance(result, dict):
                        if "detail" in result:  # Error case
                            print(f"\nError: {result['detail']}")
                        elif "response" in result:
                            print(f"\nExecution complete: {result['response']}")
                            if "filename" in result:
                                print(f"Output file: {result['filename']}")
                            if "file" in result:
                                print("\nOutput preview:")
                                print(result["file"][:500] + "..." if len(result["file"]) > 500 else result["file"])

        except Exception as e:
            self._stop_loading = True
            print("\r" + " " * 50 + "\r", end="", flush=True)
            print(f"Error: {str(e)}")
            
            # Close any open file handles
            for file in files:
                try:
                    file[1][1].close()
                except:
                    pass

    def show_help(self) -> None:
        """Display available commands."""
        print("\nAvailable Commands:")
        print(" load - Load a file")
        print(" unload - Unload a specific file")
        print(" clear - Clear all loaded files")
        print(" files - List loaded files")
        print(" functions - List available functions")
        print(" help - Show this help message")
        print(" exit - Exit the program")
        print("\nAny other input will be treated as a prompt for the assistant")

    def run(self):
        """Main loop for running the CLI."""
        print("\nGIS Assistant Terminal Interface")
        self.show_help()

        while True:
            try:
                command = input("\n(user):").strip()

                if command == "exit":
                    break
                elif command == "help":
                    self.show_help()
                elif command == "files":
                    self.list_files()
                elif command == "functions":
                    self.list_functions()
                elif command == "clear":
                    self.clear_files()
                elif command.startswith("load "):
                    self.load_file(command[5:].strip())
                elif command.startswith("unload "):
                    self.unload_file(command[7:].strip())
                else:
                    self.process_prompt(command)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {str(e)}")


if __name__ == "__main__":
    assistant = GISAssistant()
    assistant.run()