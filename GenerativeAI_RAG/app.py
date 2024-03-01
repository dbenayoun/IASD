from flask import Flask, request, render_template, jsonify
import openai
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import chromadb
import configparser


app = Flask(__name__)

# Loading OpenAI API key from configuration file
config = configparser.ConfigParser()
config.read('C:\David\ML\GenerativeAI2\config.ini') #Path to your configuration file
OPENAI_KEY =  config.get('OPENAI_API', 'OPENAI_API')

#Set your OpenAI API key here
openai.api_key = OPENAI_KEY


#instance embeddings
openai_ef = chromadb.utils.embedding_functions.OpenAIEmbeddingFunction(
                api_key=OPENAI_KEY,
                model_name="text-embedding-3-small"
            )

########chroma db 
client = chromadb.PersistentClient(path="C://David//ML//GenerativeAI2//ChromaDb")
collection = client.get_collection("tweets_chromadb_sample")


@app.route('/')
def home():
    return render_template('index.html')



@app.route('/generate-response', methods=['POST'])

def generate_response():

    tweet = request.form['tweet']
    company = request.form['company']  # Selected company from the dropdown
    k_closest = int(request.form['# closest tweets'])

    emb = openai_ef(tweet)
    
    closest_tweets= collection.query(
                    query_embeddings=emb,
                    n_results=k_closest,
                    where={"company": company},
                    )


    instruction =  f"""\
    You are a chatbot answering customer's tweet. You are working for a company called {company}. 
    You are provided with an example of a similar interaction between a customer and an agent. Reply to the customer's tweet in the same tone, structure and style than the provided example.

    """

    for k in np.arange(k_closest):
        instruction = instruction + f"""\

        #####
        Example {k +1}:
        Customer : "{closest_tweets["documents"][0][k]}"
        Agent : "{closest_tweets["metadatas"][0][k]["company_tweet"]}"
    """

    instruction = instruction + f"""\
    Tweet:
    "{tweet}"
    """

    messages = [
        {"role": "user", "content": instruction}
    ]


    response = openai.chat.completions.create(
        model= "gpt-3.5-turbo",
        messages=messages,
        seed=42,
        temperature=0.7)

    generated_text = response.choices[0].message.content



    return jsonify({'response': generated_text, 
                    'tweet': tweet,
                    'df_html': pd.DataFrame({"customer_tweet": closest_tweets["documents"][0],
                                             "company_tweet": [item["company_tweet"] for item in closest_tweets["metadatas"][0]],
                                             "company": [item["company"] for item in closest_tweets["metadatas"][0]],
                                             "distance": closest_tweets["distances"][0]}
                                             ).to_html(index=False, classes='dataframe')
                    })

    

if __name__ == '__main__':
    app.run(debug=True, port=5001)
    app.run(debug=True)
