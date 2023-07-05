def clean_academy_csvs_df(df):

    df['name'] = df['name'].str.strip().upper()

    df['trainer'].replace('Ely Kely', 'Elly Kelly', inplace=True)

    df['trainer'] = df['trainer'].str.upper()

    df[['Analytic_W1', 'Independent_W1', 'Determined_W1', 'Professional_W1',
       'Studious_W1', 'Imaginative_W1', 'Analytic_W2', 'Independent_W2',
        'Determined_W2', 'Professional_W2', 'Studious_W2', 'Imaginative_W2',
        'Analytic_W3', 'Independent_W3', 'Determined_W3', 'Professional_W3',
        'Studious_W3', 'Imaginative_W3', 'Analytic_W4', 'Independent_W4',
        'Determined_W4', 'Professional_W4', 'Studious_W4', 'Imaginative_W4',
        'Analytic_W5', 'Independent_W5', 'Determined_W5', 'Professional_W5',
        'Studious_W5', 'Imaginative_W5', 'Analytic_W6', 'Independent_W6',
        'Determined_W6', 'Professional_W6', 'Studious_W6', 'Imaginative_W6',
        'Analytic_W7', 'Independent_W7', 'Determined_W7', 'Professional_W7',
        'Studious_W7', 'Imaginative_W7', 'Analytic_W8', 'Independent_W8',
        'Determined_W8', 'Professional_W8', 'Studious_W8', 'Imaginative_W8',
        'Analytic_W9', 'Independent_W9', 'Determined_W9', 'Professional_W9',
        'Studious_W9', 'Imaginative_W9', 'Analytic_W10', 'Independent_W10',
        'Determined_W10', 'Professional_W10', 'Studious_W10', 'Imaginative_W10']] = df[['Analytic_W1', 'Independent_W1', 'Determined_W1', 'Professional_W1',
                                                                                       'Studious_W1', 'Imaginative_W1', 'Analytic_W2', 'Independent_W2',
                                                                                        'Determined_W2', 'Professional_W2', 'Studious_W2', 'Imaginative_W2',
                                                                                        'Analytic_W3', 'Independent_W3', 'Determined_W3', 'Professional_W3',
                                                                                        'Studious_W3', 'Imaginative_W3', 'Analytic_W4', 'Independent_W4',
                                                                                        'Determined_W4', 'Professional_W4', 'Studious_W4', 'Imaginative_W4',
                                                                                        'Analytic_W5', 'Independent_W5', 'Determined_W5', 'Professional_W5',
                                                                                        'Studious_W5', 'Imaginative_W5', 'Analytic_W6', 'Independent_W6',
                                                                                        'Determined_W6', 'Professional_W6', 'Studious_W6', 'Imaginative_W6',
                                                                                        'Analytic_W7', 'Independent_W7', 'Determined_W7', 'Professional_W7',
                                                                                        'Studious_W7', 'Imaginative_W7', 'Analytic_W8', 'Independent_W8',
                                                                                        'Determined_W8', 'Professional_W8', 'Studious_W8', 'Imaginative_W8',
                                                                                        'Analytic_W9', 'Independent_W9', 'Determined_W9', 'Professional_W9',
                                                                                        'Studious_W9', 'Imaginative_W9', 'Analytic_W10', 'Independent_W10',
                                                                                        'Determined_W10', 'Professional_W10', 'Studious_W10', 'Imaginative_W10']].astype(int, errors='ignore')

    return df