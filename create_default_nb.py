import json

notebook = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Datamixer Enterprise - Jupyter Workbench\n",
    "이 노트북은 Datamixer GUI와 동기화되어 있습니다. 아래 코드를 실행하여 로드된 데이터를 확인하세요."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import os\n",
    "\n",
    "# GUI에서 현재 로드된 데이터를 즉시 불러옵니다.\n",
    "if os.path.exists('last_loaded_data.parquet'):\n",
    "    df = pd.read_parquet('last_loaded_data.parquet')\n",
    "    print(f\"성공: {len(df)} 행 데이터를 로드했습니다.\")\n",
    "    display(df.head())\n",
    "else:\n",
    "    print(\"데이터가 아직 GUI에서 로드되지 않았습니다.\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}

with open(r"d:\datamixer\notebooks\Datamixer_Quickstart.ipynb", "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=1)
