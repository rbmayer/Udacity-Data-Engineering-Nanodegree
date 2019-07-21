class DataChecks:
    """Store data quality check statements and expected results"""
    empty_table_check = """SELECT (CASE WHEN COUNT(*) > 0 THEN COUNT(*)/COUNT(*) ELSE 0 END) as row_flag from {}""" 
    empty_table_check_expected_result = 1
    
    songplay_id_check = ("""
        SELECT MAX(total.songplay_id_count) as max_count 
        FROM    (SELECT COUNT(songplay_id) as songplay_id_count
                FROM songplays
                GROUP BY songplay_id) total
    """)    
    songplay_id_check_expected_result = 1  
    