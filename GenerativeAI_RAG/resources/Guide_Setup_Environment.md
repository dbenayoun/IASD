### Quickstart Guide: Setting Up a Python Flask Development Environment

#### 0. Install Git
- **Download**: Go to the [Git website](https://git-scm.com/) and download the installer for your operating system.
- **Install**: Run the installer and follow the on-screen instructions. Use the default configuration from the installation tool.

#### 1. Install Visual Studio Code (VS Code)
- **Download**: Go to the [VS Code website](https://code.visualstudio.com/) and download the installer for your operating system.
- **Install**: Run the installer and follow the on-screen instructions.
- **Verify**: Open VS Code to ensure it's installed correctly.

#### 2. Install Python
- **Download**: Visit the [Python website](https://www.python.org/downloads/) and download Python 3.12.
- **Install**: Run the installer. Ensure you check the option to "Add Python 3.12 to PATH" (environment variables).
- **Verify**: Open a command prompt or terminal and type `python --version` to check if Python is installed.

#### 3. Install Anaconda
- **Download**: Go to the [Anaconda website](https://www.anaconda.com/products/individual) and download Anaconda for Python 3.11.
- **Install**: Execute the installer and follow the instructions.
- **Create Virtual Environment**:
   - Open the Anaconda Prompt and type:
     ```bash
     conda create -n flask_env python=3.11
     ```
   - Activate the environment:
     ```bash
     conda activate flask_env
     ```

#### 4. Install Required Libraries
- **Pandas, Numpy, Flask, OpenAI, and ChromeDB**:
  - In the virtual environment, run:
    ```bash
    pip install pandas numpy flask openai chromadb jupyter ipykernel
    ```

#### 5. Install plugins in VS Code
- **Install Python Plugin**:
  - Go to Extensions and search for "Python".
  - Install the Python extension.

- **Install Jupyter Plugin**:
  - Open VS Code.
  - Go to Extensions (sidebar) and search for "Jupyter".
  - Install the Jupyter extension.


#### 6. Verify Installation
- Create a simple Jupyter notebook in VS Code in folder `notebooks` and try creating a jupyter notebook (`test_noteobok_env.ipynb`)
- Click on the top right `Select kernel`, select `flask_env` as a kernel
- Import Flask, Pandas, OpenAI etc., to ensure everything is installed correctly.

#### 7. Starting Your First Flask Project
- **Select `flask_env` as python interpreter**
  - Use the Python: Select Interpreter command from the Command Palette (Ctrl+Shift+P).
  - Select `flask_env`
- **Create a New Python File** (e.g., `test_flak_app.py`) in the `src` folder.
- **Write a Basic Flask App**:
  ```python
  from flask import Flask
  app = Flask(__name__)

  @app.route('/')
  def hello_world():
      return 'Hello, World!'

  if __name__ == '__main__':
      app.run(debug=True)
  ```
- **Run Your Flask App**:
  - Open a terminal in VS Code.
  - Ensure your virtual environment is active.
  - Run the command `python app.py`.
  - Open a web browser and go to `http://127.0.0.1:5000/` to see your Flask app.
