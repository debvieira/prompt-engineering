# Text Classification Project: sentiment analysis
This text classification project uses BERT to process and classify text data. The objective of this project is to leverage the powerful BERT model to accurately classify textual data into predefined categories. This involves fine-tuning the BERT model on a labeled dataset, optimizing its performance, and evaluating its effectiveness.

### Files
- text_classification_bert.ipynb

# text_classification_bert.ipynb
This script begins by importing the necessary libraries for text processing and model building: transformers, torch, and sklearn. The BERT model and tokenizer from the Hugging Face library are used for text classification tasks. The data consists of labeled text samples which will be processed and classified through various steps, including cleaning and tokenizing the text, formatting inputs for BERT, training the model, and evaluating its performance. It includes a combination of sentiment analysis results with different NLP methods to visualize the relationship between different sentiment scores.
- `Torch`: Used for tensor operations and computations in the PyTorch framework.
- `NLTK`: Used for natural language processing, including tokenization, part-of-speech (POS) tagging, and named entity recognition (NER).
- `Matplotlib` and `Seaborn`: Used for data visualization.
- `Transformers`: Used for loading pre-trained models and tokenizers from the Hugging Face library.
- `Scipy`: Used for the softmax function that calculates the softmax of scores.
- `TQDM`: Used for displaying progress bars.
- `Tokenization of the example sentence`: Performing POS tagging and named entity recognition (NER).
- `VADER`: Using VADER to calculate sentiment scores (positive, negative, neutral, and compound).
- `RoBERTa`: Loading the pre-trained RoBERTa tokenizer and model for sentiment analysis.
- `Sentiment Analysis Pipeline`: Using the Hugging Face library's pipeline for sentiment analysis on texts.