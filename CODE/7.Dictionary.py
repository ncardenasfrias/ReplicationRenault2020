# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

@author: ncardenafria
"""

import pandas as pd
import re
from pymongo import MongoClient

# Expanded sentiment dictionary with additional financial terms
financial_sentiment_dict = {
    # Positive words
    'buy': 'positive', 'growth': 'positive', 'earnings': 'positive', 'bull': 'positive',
    'rally': 'positive', 'profit': 'positive', 'gain': 'positive', 'breakout': 'positive',
    'bullish': 'positive', 'surge': 'positive', 'high': 'positive', 'win': 'positive',
    'success': 'positive', 'benefit': 'positive', 'up': 'positive', 'long': 'positive',
    'support': 'positive', 'recovery': 'positive', 'advantage': 'positive', 'top': 'positive',
    'best': 'positive', 'boost': 'positive', 'improvement': 'positive', 'leader': 'positive',
    'surpass': 'positive', 'exceed': 'positive', 'peak': 'positive', 'optimistic': 'positive',
    'reward': 'positive', 'strong': 'positive', 'expansion': 'positive', 'thrive': 'positive',
    'outperform': 'positive', 'upgrade': 'positive', 'success': 'positive', 'profitable': 'positive',
    # Additional positive words
    'accelerate': 'positive', 'accomplishment': 'positive', 'achievements': 'positive', 'advance': 'positive',
    'advancing': 'positive', 'advantageous': 'positive', 'affirm': 'positive', 'affirmation': 'positive',
    'affluent': 'positive', 'aggregate': 'positive', 'ahead': 'positive', 'align': 'positive', 'aligned': 'positive',
    'ascending': 'positive', 'aspiration': 'positive', 'aspirations': 'positive', 'assure': 'positive', 'assured': 'positive',
    'attain': 'positive', 'attainment': 'positive', 'attractive': 'positive', 'augment': 'positive', 'augmented': 'positive',
    'auspicious': 'positive', 'authentic': 'positive', 'authoritative': 'positive', 'authorize': 'positive',
    'bolster': 'positive', 'boom': 'positive', 'booming': 'positive', 'breakthrough': 'positive', 'bright': 'positive',
    'brilliant': 'positive', 'bull-run': 'positive', 'capitalize': 'positive', 'captivating': 'positive',
    'celebrate': 'positive', 'champion': 'positive', 'climb': 'positive', 'commanding': 'positive', 'compelling': 'positive',
    'competitive': 'positive', 'complete': 'positive', 'comprehensive': 'positive', 'conclusive': 'positive',
    'confidence': 'positive', 'confident': 'positive', 'conquer': 'positive', 'considerable': 'positive', 'constructive': 'positive',
    'consummate': 'positive', 'continuous': 'positive', 'contribute': 'positive', 'convincing': 'positive', 'cornerstone': 'positive',
    'correct': 'positive', 'culminate': 'positive', 'culmination': 'positive', 'dominate': 'positive', 'domination': 'positive',
    'earn': 'positive', 'effective': 'positive', 'efficacy': 'positive', 'efficient': 'positive', 'elevate': 'positive',
    'emerging': 'positive', 'empower': 'positive', 'enable': 'positive', 'enrich': 'positive', 'enrichment': 'positive',
    'ensure': 'positive', 'enthusiasm': 'positive', 'enthusiastic': 'positive', 'excel': 'positive', 'excellence': 'positive',
    'exceptional': 'positive', 'exhilarate': 'positive', 'expand': 'positive', 'expedite': 'positive', 'expert': 'positive',
    'expertise': 'positive', 'extensive': 'positive', 'flourish': 'positive', 'flourishing': 'positive', 'foster': 'positive',
    'freedom': 'positive', 'gainful': 'positive', 'generating': 'positive', 'generous': 'positive', 'genius': 'positive',
    'goal': 'positive', 'good': 'positive', 'grand': 'positive', 'gratify': 'positive', 'great': 'positive',
    'greater': 'positive', 'greatest': 'positive', 'growth-oriented': 'positive', 'guarantee': 'positive', 'honored': 'positive',
    'ideal': 'positive', 'immense': 'positive', 'impactful': 'positive', 'improve': 'positive', 'improved': 'positive',
    'improving': 'positive', 'increase': 'positive', 'influential': 'positive', 'innovate': 'positive', 'innovative': 'positive',
    'insightful': 'positive', 'inspire': 'positive', 'inspiring': 'positive', 'integrity': 'positive', 'intelligent': 'positive',
    'intensify': 'positive', 'interest': 'positive', 'invest': 'positive', 'invigorate': 'positive', 'leading': 'positive',
    'lucrative': 'positive', 'master': 'positive', 'masterful': 'positive', 'masterpiece': 'positive', 'maximize': 'positive',
    'milestone': 'positive', 'miracle': 'positive', 'modernize': 'positive', 'novel': 'positive', 'optimal': 'positive',
    'outshine': 'positive', 'paradigm': 'positive', 'pinnacle': 'positive', 'pioneer': 'positive', 'precise': 'positive',
    'preeminent': 'positive', 'premium': 'positive', 'prestige': 'positive', 'prestigious': 'positive', 'proactive': 'positive',
    'profitability': 'positive', 'profound': 'positive', 'progress': 'positive', 'prolific': 'positive', 'prominent': 'positive',
    'promise': 'positive', 'promising': 'positive', 'propel': 'positive', 'prosper': 'positive', 'prosperity': 'positive',
    'prosperous': 'positive', 'quality': 'positive', 'quantum': 'positive', 'quintessential': 'positive', 'remarkable': 'positive',
    'renowned': 'positive', 'resilient': 'positive', 'resolve': 'positive', 'revolutionary': 'positive', 'rich': 'positive',
    'robust': 'positive', 'savior': 'positive', 'savour': 'positive', 'secure': 'positive', 'secured': 'positive',
    'significance': 'positive', 'significant': 'positive', 'skyrocket': 'positive', 'sophisticated': 'positive',
    'spearhead': 'positive', 'spectacular': 'positive', 'spike': 'positive', 'stability': 'positive', 'stable': 'positive',
    'staple': 'positive', 'stellar': 'positive', 'strategic': 'positive', 'streamline': 'positive', 'strengthen': 'positive',
    'stronghold': 'positive', 'successor': 'positive', 'superior': 'positive', 'surmount': 'positive', 'sustain': 'positive',
    'sustainable': 'positive', 'tailwind': 'positive', 'triumph': 'positive', 'triumphant': 'positive', 'trust': 'positive',
    'unbeatable': 'positive', 'unmatched': 'positive', 'unprecedented': 'positive', 'upbeat': 'positive', 'uplift': 'positive',
    'uplifting': 'positive', 'upsurge': 'positive', 'valuable': 'positive', 'value': 'positive', 'vanguard': 'positive',
    'vantage': 'positive', 'vast': 'positive', 'venerable': 'positive', 'victory': 'positive', 'vigorous': 'positive',
    'vitalize': 'positive', 'wealth': 'positive', 'wealthy': 'positive', 'winning': 'positive', 'wisdom': 'positive', 
    'wise': 'positive', 'wonderful': 'positive', 'yield': 'positive', 'zest': 'positive',
    # Negative words
    'loss': 'negative', 'bear': 'negative', 'crash': 'negative', 'recession': 'negative',
    'low': 'negative', 'risk': 'negative', 'down': 'negative', 'drop': 'negative',
    'decline': 'negative', 'dip': 'negative', 'decrease': 'negative', 'fall': 'negative',
    'plunge': 'negative', 'bearish': 'negative', 'fail': 'negative', 'falling': 'negative',
    'sell': 'negative', 'short': 'negative', 'liquidate': 'negative', 'bankruptcy': 'negative',
    'debt': 'negative', 'downgrade': 'negative', 'challenge': 'negative', 'breakdown': 'negative',
    'worse': 'negative', 'volatile': 'negative', 'bottom': 'negative', 'lag': 'negative',
    'problem': 'negative', 'suffer': 'negative', 'negative': 'negative', 'losses': 'negative',
    'underperform': 'negative', 'threat': 'negative', 'withdraw': 'negative', 'closure': 'negative',
    'trouble': 'negative', 'struggle': 'negative', 'downturn': 'negative', 'slump': 'negative',
    'stagnant': 'negative', 'stagnation': 'negative', 'layoff': 'negative', 'layoffs': 'negative',
    'fired': 'negative', 'firing': 'negative', 'redundancy': 'negative', 'redundancies': 'negative',
    'cutback': 'negative', 'cutbacks': 'negative', 'downsize': 'negative', 'downsizing': 'negative',
    'retract': 'negative', 'retraction': 'negative', 'withdrawal': 'negative', 'cancellation': 'negative',
    'cancel': 'negative', 'canceled': 'negative', 'suspension': 'negative', 'suspend': 'negative',
    'delay': 'negative', 'delayed': 'negative', 'postpone': 'negative', 'postponed': 'negative',
    'halt': 'negative', 'halted': 'negative', 'stalemate': 'negative', 'impasse': 'negative',
    'regression': 'negative', 'reversal': 'negative', 'revert': 'negative', 'reverted': 'negative',
    'failures': 'negative', 'failure': 'negative', 'collapse': 'negative', 'collapsed': 'negative',
    'shrinking': 'negative', 'shrink': 'negative', 'shrank': 'negative', 'contract': 'negative',
    'contracted': 'negative', 'deteriorate': 'negative', 'deterioration': 'negative', 'weaken': 'negative',
    'weakened': 'negative', 'devaluation': 'negative', 'devalue': 'negative', 'penalty': 'negative',
    'penalties': 'negative', 'fine': 'negative', 'fines': 'negative', 'sanction': 'negative',
    'sanctions': 'negative', 'dissent': 'negative', 'conflict': 'negative', 'dispute': 'negative',
    'litigation': 'negative', 'litigate': 'negative', 'sue': 'negative', 'sued': 'negative',
    'lawsuit': 'negative', 'injunction': 'negative', 'incompetence': 'negative', 'incompetent': 'negative',
    'obsolete': 'negative', 'obsolescence': 'negative', 'riskier': 'negative', 'riskiest': 'negative',
    'hindrance': 'negative', 'obstacle': 'negative', 'block': 'negative', 'blockade': 'negative',
    'regulate': 'negative', 'regulated': 'negative', 'regulation': 'negative', 'restrict': 'negative',
    'restricted': 'negative', 'restriction': 'negative', 'tightening': 'negative', 'tighten': 'negative',
    'tougher': 'negative', 'toughest': 'negative', 'hardship': 'negative', 'hard': 'negative',
    'difficult': 'negative', 'difficulty': 'negative', 'uncertainty': 'negative', 'uncertain': 'negative',
    'unpredictable': 'negative', 'unstable': 'negative', 'instability': 'negative', 'flaw': 'negative',
    'flaws': 'negative', 'flawed': 'negative', 'deficit': 'negative', 'deficits': 'negative',
    'loss-making': 'negative', 'unprofitable': 'negative', 'declining': 'negative', 'dwindle': 'negative',
    'dwindling': 'negative', 'erosion': 'negative', 'erode': 'negative', 'eroded': 'negative'
}

def determine_sentiment(text):
    words = re.findall(r'\w+', text.lower())  # Improved word extraction
    sentiment_score = 0
    for word in words:
        if word in financial_sentiment_dict:
            sentiment = financial_sentiment_dict[word]
            if sentiment == 'positive':
                sentiment_score += 1
            elif sentiment == 'negative':
                sentiment_score -= 1
    return 'positive' if sentiment_score >= 0 else 'negative'

def process_data(data):
    df = pd.DataFrame(data)
    df['custom_sentiment'] = df['content'].apply(determine_sentiment)
    return df

# Connect to MongoDB
uri = "mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"
client = MongoClient(uri)

try:
    db = client["StockTwits"]
    cursor = db["twits"].find({}, {'content': 1, '_id': 0}).batch_size(500)
    data = []
    results = pd.DataFrame()
    try:
        for document in cursor:
            data.append(document)
            if len(data) % 1000 == 0:
                results = pd.concat([results, process_data(data)], ignore_index=True)
                data = []
        if data:
            results = pd.concat([results, process_data(data)], ignore_index=True)
    finally:
        cursor.close()
    print("Data fetched and processed successfully!")
    print(results['custom_sentiment'].value_counts())
finally:
    client.close()

#output 
Data fetched and processed successfully!
positive    870205
negative    136965
Name: custom_sentiment, dtype: int64


