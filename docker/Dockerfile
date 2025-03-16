#Use an official image from language
from python:3.12.3 
# set the working directories
WORKDIR /app
# Copy requirements and main script files
COPY requirements.txt .
COPY HZZAnalysis.py .
COPY infofile.py .
# install dependencies
run pip install --no-cache-dir -r requirements.txt

# Create an output directory for plots
RUN mkdir -p /app/output

# Run the script
ENTRYPOINT [ "python" , "HZZAnalysis.py" ]



# Make port 8888 available to the world outside this container (for Jupyter Notebook)
#EXPOSE 8888

