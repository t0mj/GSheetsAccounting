#!/usr/bin/env python3.6
"""
Runs as a cron job to grab unprocessed transactions from the MTL and post them to their
corresponding file and sheet.
"""
from datetime import datetime
from sheets import append_data, get_sheet, update_cells
import pandas as pd
import yaml


SHEETS = yaml.safe_load(open('config.yml'))


def adjust_debits_credits(df):
    credits = df[df['Transaction Type'] == 'Deposit']
    debits = df[df['Transaction Type'] == 'Withdrawal']
    credits = credits.rename(columns={'Amount': 'Credit'})
    debits = debits.rename(columns={'Amount': 'Debit'})
    if len(credits) and len(debits):
        credits.loc[:, 'Debit'] = None
        debits.loc[:, 'Credit'] = None
        df = pd.concat([credits, debits])
    elif len(credits):
        df = credits
        df['Debit'] = None
    else:
        df = debits
        df['Credit'] = None
    return df.sort_index()


def find_unprocessed_txns(mtl):
    headers = mtl.pop(0)
    df = pd.DataFrame(mtl)
    df.columns = headers
    df = df[df.Processed != 'Yes']
    if len(df):
        df = adjust_debits_credits(df)
    return df


def mark_processed(mtl, idx_of_proc_txns):
    data = []
    for idx in idx_of_proc_txns:
        # Add 2 to idx for headers and popping them
        data.append({'range': SHEETS['MTL']['name'] + 'H' + str(idx + 2), 'values': [['Yes']]})

    result = update_cells(SHEETS['MTL']['id'], data)
    print('--------')
    print(f"{result['totalUpdatedRows']} rows marked processed on the MTL")


def post_account(txn_df):
    # Find all months needed
    txn_df['sheet'] = None
    for idx, dt in txn_df['Transaction Date'].to_dict().items():
        mo, day, yr = dt.split("/")
        txn_df.loc[idx]['sheet'] = datetime(int(yr), int(mo), int(day)).strftime('%b %Y')
    sheets_needed = set(txn_df['sheet'].tolist())
    idx_of_proc_txns = []
    for sheet_name in sheets_needed:
        idx_of_proc_txns += post_to_month(txn_df[txn_df['sheet'] == sheet_name],
                                          account, sheet_name)
    return idx_of_proc_txns


def post_to_month(sheet_df, account, sheet_name):
    # Reorder and sort columns for sheets
    # Get columns from current sheet to sort in correct order
    acct = SHEETS[account]['id']
    sheet = sheet_name + SHEETS[account]['range']
    sheet_data = get_sheet(acct, sheet)
    sheet_cols = sheet_data.pop(0)
    # Find associated account txns to push
    txns_to_push = sheet_df[sheet_cols]
    txns_to_push.fillna(0, inplace=True)
    result = append_data(acct, sheet, txns_to_push.values.tolist())
    print(f"{result['updates']['updatedRows']} rows pushed to {account} {sheet_name}")

    return txns_to_push.index.tolist()


if __name__ == '__main__':
    mtl = get_sheet(SHEETS['MTL']['id'], SHEETS['MTL']['name'] + SHEETS['MTL']['range'])
    unprocessed_txns = find_unprocessed_txns(mtl)
    if len(unprocessed_txns):
        accounts_to_process = set(unprocessed_txns['Account'].tolist())
        for account in accounts_to_process:
            account_txns_df = unprocessed_txns[unprocessed_txns['Account'] == account]
            if len(account_txns_df):
                print(f"Processing {len(account_txns_df)} {account} transactions")
                idx_of_proc_txns = post_account(account_txns_df)
                mark_processed(mtl, idx_of_proc_txns)
            else:
                print(f"{account} has no new transactions")
        print('Finished')
    else:
        print("MTL has no new transactions to process")
