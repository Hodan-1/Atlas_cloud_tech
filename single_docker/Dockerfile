#Use an official image from language
FROM python:3.12.3 
# set the working directories
WORKDIR /app
# Copy requirements and main script files
COPY requirements.txt .
COPY HZZAnalysis.py .
COPY infofile.py .
# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the script
ENTRYPOINT [ "python" , "HZZAnalysis.py" ]



# Make port 8888 available to the world outside this container (for Jupyter Notebook)
#EXPOSE 8888

