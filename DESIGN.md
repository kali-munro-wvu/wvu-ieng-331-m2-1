\# Design Rationale



\## Parameter Flow



Trace how a command-line argument (e.g., `--seller-state SP`) travels through your code:

which function parses it, how it reaches the queries module, and how it changes the SQL that runs.

Include the actual function names and parameter names from your code.



\## SQL Parameterization



Pick one of your `.sql` files and explain:

\- What the raw SQL looks like (with `$1`, `$2` placeholders)

\- How `queries.py` reads the file and passes Python values as parameters

\- Why parameterized queries are used instead of f-strings

\- Why SQL lives in `.sql` files rather than inline in Python



\## Validation Logic



For each validation check in `validation.py`, explain:

\- What it checks and why that check matters

\- What happens if it fails (halt vs. warning)

\- How you chose your thresholds (e.g., why 1,000 rows as a minimum)



\## Error Handling



Pick 2 specific `try/except` blocks in your code and explain:

\- What exception you catch and why that specific type

\- What the code does when the exception is raised

\- What would happen to the user if you used a bare `except:` instead



\## Scaling \& Adaptation



Answer both:

1\. If the Olist dataset grew to 10 million orders, what part of your pipeline would break or slow down first? What would you change?

2\. If you needed to add a third output format (e.g., a JSON API response), where in your code would you add it and what functions would you modify?

