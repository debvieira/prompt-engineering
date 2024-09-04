import torch
from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from scipy.special import softmax
from transformers import pipeline

def polarity_scores_roberta(example):
    """
    Computes the polarity scores for a given text using a RoBERTa model.

    Parameters:
    example (str): The input text for analysis.

    Returns:
    dict: A dictionary with keys 'roberta_neg', 'roberta_neu', and 'roberta_pos'
          representing the negative, neutral, and positive sentiment scores.
    """
    encoded_text = tokenizer(example, return_tensors='pt')
    output = model(**encoded_text)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    scores_dict = {
        'roberta_neg': scores[0],
        'roberta_neu': scores[1],
        'roberta_pos': scores[2]
    }
    return scores_dict