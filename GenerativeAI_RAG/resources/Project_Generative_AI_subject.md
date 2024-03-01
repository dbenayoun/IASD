# Development of an End-to-End Web Application Leveraging Retrieval Augmented Generation (RAG) and OpenAI's API with Enterprise Data

**Course**: Generative AI  
**University**: Dauphine-PSL University

## Project Description

This project aims to develop a robust web application that integrates the principles of Retrieval Augmented Generation (RAG) and OpenAI's API. The application will utilize enterprise data to demonstrate the potential of Generative AI in enhancing productivity and task automatization, especially in customer interaction and customer service.

## Datasets

Simulated enterprise data will be provided, which includes:
- Twitter’s customer support data: tweets from customers and replies from customer service
- ~~Customer’s emails and customer service agent’s responses~~
- ~~Customer service call transcripts~~
- ~~Company Knowledge base PDF document~~
- ~~Company’s FAQ~~

## Objectives

1. **Product Brainstorming:** 
   - Analysis and understanding of the available data. 
   - Identifying opportunities where Generative AI can provide value.
   - Exploration of potential applications using RAG and Generative AI on those data.
   - Choose an idea of web application to implement.

2. **Data Retrieval and Formatting:**
   - Extracting, cleaning, and organizing the provided data.
   - Formatting the data to be compatible with a RAG system.
   - Start with the sample data `twitter_data_clean_sample`.

3. **Development:**
   - Architectural design of the web application.
   - Building functional back-end components of the application using Flask in Python, integrating Azure OpenAI's API and a RAG system.
   - Building a user-friendly front-end using HTML, CSS, and Javascript.
   - Storing data in a vector database, such as ChromaDB.
   - Version control with Git.

4. **Deployment:**
   - Deploying the application on an appropriate web platform.

## Expected Deliverable

- A fully operational web application that demonstrates the practical application of Retrieval Augmented Generation and generative AI using enterprise data.
- The project is available on GitHub.

## Project Timeline

The project’s GitHub needs to be sent before 15/03/2024.

## Getting Started

1. Fill the form to get your OpenAI API key

https://docs.google.com/spreadsheets/d/1Bq_fIELFZWANblbx1UL3CM1rpgHX-mx_8yABH-mYgMQ/edit?usp=sharing

#### Please only use the following models from OpenAI (the other models, especially GPT-4, are too expensive and will result in consuming all the API Credit):

- `gpt-3.5-turbo` for ChatCompletion
- `text-embedding-3-small` for Embeddings

2. Install the working environment following the guide

`resources\Guide_Setup_Environment.md`

3. Create a GitHub account

4. Fork the course repository into your GitHub account

Link : https://github.com/End2EndAI/Generative-AI-Module-Dauphine

Click on the `Fork` button on the page above, while being connected to your GitHub account.

5. Clone the repository on your laptop

You should have git installed on your laptop. Go to a folder and use the command `SHIFT + Right click`. Click on `Git Bash Here`.

In the opened terminal, use the following command to clone the repository : `git clone https://github.com/End2EndAI/Generative-AI-Module-Dauphine.git` 

6. Open VSCode and open the repository

Click on `File`, `Open Folder`.

7. Setup your virtual env in VSCode

Click on `Help`, `Show All Commands`. Type `Python: Select Interpreter` and choose your virtual env (`flask_env` from the guide).

8. Test your environment

Open the files `notebooks\getting_started.ipynb` and `test_flask_app.py`, and run them, to see if everything is well configured.

10. Check the data

Use the sample data first `data\twitter_data_clean_sample.csv`. 

#### Warning : there are a lot of data in the full csv data `data\twitter_data_clean.csv`, please be careful especially when processing that data with OpenAI to not consume all the API Credit.

11. You are ready to go 🥳


## Suggestion of the way of work

- Start with comprehending the data using the sample file and establishing a clear objective for your application.

- Initially, construct a basic prototype of the front-end interface, utilizing ChatGPT for this purpose. 

- Once the primary features of the front-end are operational, proceed to develop a straightforward Flask back-end. This could start as simply as returning the input message. Ensure the Flask server is operational and effectively communicating with the front-end.

- Next, enhance the Flask back-end by integrating the Retrieval-Augmented Generation (RAG) system. Start by utilizing the provided sample file `twitter_data_clean_sample.csv` to enable the system to identify and respond with the most relevant tweet, using GPT-generated answers. Initially, employ an Excel file for storing embeddings, progressing later to a more sophisticated vector database solution, such as ChromaDB.

#### Please only use the following models from OpenAI (the other models, especially GPT-4, are too expensive and will result in consuming all the API Credit):

- `gpt-3.5-turbo` for ChatCompletion
- `text-embedding-3-small` for Embeddings

- Further refine the application by improving the prompts, retrieval methods, and front-end features, such as adding translation and text reformulation capabilities. Then use the full data `data\twitter_data_clean.csv` in the app.

#### Warning : There are a lot of data in the full csv data `data\twitter_data_clean.csv`, please be careful especially when processing that data with OpenAI to not consume all the API Credit.

- Evaluate your system using the evaluation dataset `data\twitter_data_clean_eval.csv`.

- Finally, focus on deploying your web application. For hosting the Flask application, consider using a free platform like PythonAnywhere, which allows for hosting, running, and coding Python in a cloud environment. 

## Resources

1. [Github of the Translation App for the code structure](https://github.com/End2EndAI/travel-ai-translator)
2. [Guide to use the OpenAI API](https://platform.openai.com/docs/overview)
3. [Guide to build the RAG system](https://platform.openai.com/docs/tutorials/web-qa-embeddings)
5. [Guide to implement a ChromaDB vector database](https://docs.trychroma.com/getting-started) 
6. [Free hosting for Flask app](https://www.pythonanywhere.com)
7. How to use Git and how to push on GitHub from Visual Studio: Ask ChatGPT 😉
