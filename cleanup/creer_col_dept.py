def creer_col_dept(df):

    import pandas as pd

    # List of mainland departments
    dept_mainland = [
        "01", "02", "03", "04", "05", "06", "07", "08", "09",
        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
        "2A", "2B", "21", "22", "23", "24", "25", "26", "27", "28", "29",
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
        "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
        "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
        "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
        "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
        "90", "91", "92", "93", "94", "95", "971", "972", "973", "974", "976"
    ]

    removed_count = 0

    # Convert 'lieuexecution_code' to string to avoid issues with non-string values
    df['lieuexecution_code'] = df['lieuexecution_code'].astype(str)

    print(f'Initial row count: {df.shape[0]}')
    print("")

    # Filter out all values that contain non-numeric characters
    print('Filter out all values that contain non-numeric characters')
    df_filtered = df[~(df['lieuexecution_code'].str.contains(r'\D'))]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0]}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0]

    # Filter out all values that are over 5 digits
    print('Filter out all values that are over 5 digits')
    df_filtered = df_filtered[~(df_filtered['lieuexecution_code'].str.len() > 5)]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    # Filter out all values that have 1 digit
    print('Filter out all values that have 1 digit')
    df_filtered = df_filtered[~(df_filtered['lieuexecution_code'].str.len() == 1)]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    # Remove values that have exactly 3 characters, unless they are '971', '972', '973', '974', or '976'
    print("Remove values that have exactly 3 characters, unless they are '971', '972', '973', '974', or '976'")
    allowed_values = ['971', '972', '973', '974', '976']
    df_filtered = df_filtered[~((df_filtered['lieuexecution_code'].str.len() == 3) & (~df_filtered['lieuexecution_code'].isin(allowed_values)))]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    # Remove values that have exactly 4 characters
    print('Remove values that have exactly 4 characters')
    df_filtered = df_filtered[~(df_filtered['lieuexecution_code'].str.len() == 4)]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    # Remove values that begin with '96', '98', or '99'
    print("Remove values that begin with '96', '98', or '99'")
    df_filtered = df_filtered[~(df_filtered['lieuexecution_code'].str.startswith(('96', '98', '99')))]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count after filtering: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    # Function to determine the 'dept' value
    def determine_dept(lieuexecution_code):
        lieuexecution_code = str(lieuexecution_code)
        if len(lieuexecution_code) <= 3:
            return lieuexecution_code
        elif lieuexecution_code.startswith('97'):
            return lieuexecution_code[:3]
        else:
            return lieuexecution_code[:2]

    # Apply the function to create the new 'dept' column
    print("Create new dept column from starting values of filtered lieuexecution_code column")
    df_filtered['dept'] = df_filtered['lieuexecution_code'].apply(determine_dept)
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Row count in new dept: {df_filtered.shape[0]}')
    print(f'Percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')
    removed_count += df.shape[0] - df_filtered.shape[0] - removed_count

    print("Final filter:  Remove rows where 'dept' value isn't in 'dept_mainland' list")
    df_filtered = df_filtered[df_filtered['dept'].isin(dept_mainland)]
    print(f'Rows removed: {df.shape[0] - df_filtered.shape[0] - removed_count}')
    print(f'Final row count: {df_filtered.shape[0]}')
    print(f'Final percent of original: {df_filtered.shape[0] / df.shape[0] * 100}%\n')

    return df_filtered


