# generate_report.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import os
import numpy as np
from datetime import datetime

try:
    # Connect to MySQL
    conn = mysql.connector.connect(
        host='localhost',
        user='hamza',
        password='shams1098',
        database='finance_db'
    )

    # Load transaction data
    transactions_df = pd.read_sql('SELECT * FROM transactions', conn)

    # Separate Kaggle and Bank Simulation datasets
    kaggle_df = transactions_df[transactions_df['data_source'] == 'kaggle_fraud']
    bank_df = transactions_df[transactions_df['data_source'] == 'bank_simulation']

    # Generate and print summary statistics
    print("\nBasic Statistics:")
    print(f"Total transactions: {len(transactions_df)}")
    print(f"Kaggle transactions: {len(kaggle_df)}")
    print(f"Bank transactions: {len(bank_df)}")
    print(f"Fraudulent transactions: {transactions_df['is_fraud'].sum()}")
    print(f"Fraud rate: {transactions_df['is_fraud'].mean() * 100:.2f}%")

    # Summary statistics grouped
    summary_df = transactions_df.groupby(['is_fraud', 'data_source']).agg(
        transaction_count=('transaction_amount', 'count'),
        total_amount=('transaction_amount', 'sum'),
        avg_amount=('transaction_amount', 'mean'),
        min_amount=('transaction_amount', 'min'),
        max_amount=('transaction_amount', 'max')
    ).reset_index()
    print("\nTransaction Summary:")
    print(summary_df)

    # Create visualizations directory
    report_dir = '/home/hamza/DE/20212619_DS463_final/financial-transactions-project/reports'
    os.makedirs(report_dir, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')

    # Paths for plots
    fraud_plot = f"combined_fraud_bar_{today}.png"
    amount_plot = f"amount_distribution_log_{today}.png"
    source_plot = f"source_distribution_{today}.png"
    avg_plot = f"avg_by_source_fraud_{today}.png"

    # Plot 1: Fraud vs Non-Fraud Distribution
    plt.figure(figsize=(10, 6))
    fraud_counts = transactions_df['is_fraud'].value_counts().sort_index()
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(10, 8), gridspec_kw={'height_ratios': [4, 1], 'hspace': 0.05})
    ax1.bar(0, fraud_counts.get(0, 0), color='#66b3ff', width=0.5)
    ax1.set_ylim(bottom=fraud_counts.get(0, 0) * 0.9)
    ax1.spines['bottom'].set_visible(False)
    ax1.text(0, fraud_counts.get(0, 0) * 0.95, f"{fraud_counts.get(0, 0):,}", ha='center', color='white')
    ax2.bar(1, fraud_counts.get(1, 0), color='#ff9999', width=0.5)
    ax2.set_ylim(top=fraud_counts.get(1, 0) * 1.2)
    ax2.spines['top'].set_visible(False)
    ax2.text(1, fraud_counts.get(1, 0) * 1.05, f"{fraud_counts.get(1, 0):,}", ha='center')
    d = .015
    kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
    ax1.plot((-d, +d), (-d, +d), **kwargs)
    ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)
    kwargs.update(transform=ax2.transAxes)
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
    ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)
    ax1.set_title('Fraud vs Non-Fraud Transactions (Combined)')
    ax2.set_xlabel('Transaction Type')
    ax1.set_ylabel('Number of Transactions')
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels(['Non-Fraud', 'Fraud'])
    fraud_pct = transactions_df['is_fraud'].mean() * 100
    ax2.annotate(f'Fraud Rate: {fraud_pct:.4f}%', xy=(1, fraud_counts.get(1, 0)/2), xytext=(1.3, fraud_counts.get(1, 0)/2),
                 arrowprops=dict(facecolor='black', shrink=0.05, width=1.5, headwidth=8), fontsize=12, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, fraud_plot))

    # Plot 2: Amount Distribution by Fraud Status (log scale)
    plt.figure(figsize=(10, 6))
    plt.yscale('log')
    sns.boxplot(x='is_fraud', y='transaction_amount', data=transactions_df)
    plt.title('Transaction Amount Distribution by Fraud Status (Log Scale)')
    plt.xlabel('Is Fraud (0=No, 1=Yes)')
    plt.ylabel('Transaction Amount ($) - Log Scale')
    medians = transactions_df.groupby('is_fraud')['transaction_amount'].median()
    for i, median in enumerate(medians):
        plt.text(i, median * 1.2, f'Median: ${median:.2f}', ha='center')
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, amount_plot))

    # Plot 3: Data Source Distribution
    plt.figure(figsize=(10, 6))
    transactions_df['data_source'].value_counts().plot(kind='pie', autopct='%1.1f%%', startangle=90,
                                                       colors=['#66b3ff', '#ff9999'], explode=[0.05, 0.05])
    plt.title('Transaction Distribution by Data Source')
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, source_plot))

    # Plot 4: Avg Transaction Amount by Source and Fraud Status
    plt.figure(figsize=(12, 6))
    avg_by_source_fraud = transactions_df.groupby(['data_source', 'is_fraud'])['transaction_amount'].mean().reset_index()
    avg_by_source_fraud['is_fraud'] = avg_by_source_fraud['is_fraud'].map({0: 'Non-Fraud', 1: 'Fraud'})
    plt.yscale('log')
    sns.barplot(x='data_source', y='transaction_amount', hue='is_fraud', data=avg_by_source_fraud,
                palette=['#66b3ff', '#ff9999'])
    plt.title('Average Transaction Amount by Source and Fraud Status (Log Scale)')
    plt.xlabel('Data Source')
    plt.ylabel('Average Amount ($) - Log Scale')
    plt.legend(title='Transaction Type')
    for i, row in enumerate(avg_by_source_fraud.itertuples()):
        plt.text(i % 2 - 0.2 if i < 2 else i % 2 + 0.2, row.transaction_amount * 1.1, 
                 f"${row.transaction_amount:.2f}", ha='center')
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, avg_plot))

    # Generate HTML report
    html_path = os.path.join(report_dir, f'fraud_report_{today}.html')
    with open(html_path, 'w') as f:
        f.write("""
        <html><head><title>Fraud Analysis Report</title></head><body>
        <h1>Fraud Analysis Report</h1>
        <p><strong>Total Transactions:</strong> {}<br>
        <strong>Kaggle:</strong> {} &nbsp;&nbsp; <strong>Bank:</strong> {}<br>
        <strong>Fraud Rate:</strong> {:.2f}%</p>
        <h2>Summary Table</h2>
        {}<br><br>
        <h2>Visualizations</h2>
        <h3>Fraud vs Non-Fraud</h3>
        <img src='{}' width='800'><br>
        <h3>Amount Distribution by Fraud</h3>
        <img src='{}' width='800'><br>
        <h3>Data Source Distribution</h3>
        <img src='{}' width='600'><br>
        <h3>Average Amount by Source and Fraud</h3>
        <img src='{}' width='800'>
        </body></html>
        """.format(
            len(transactions_df),
            len(kaggle_df),
            len(bank_df),
            fraud_pct,
            summary_df.to_html(index=False),
            fraud_plot,
            amount_plot,
            source_plot,
            avg_plot
        ))

    print(f"Report generated: {html_path}")

except Exception as e:
    print(f"Error analyzing datasets: {str(e)}")
