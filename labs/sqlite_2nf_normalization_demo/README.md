SQLite Normalization (2NF) Demo

This lab demonstrates how a table violating Second Normal Form (2NF)
can be normalized by separating repeated author information into a
dedicated table.

Steps demonstrated:

1. Create a denormalized BookShop table
2. Identify redundant author data
3. Create BookShop_AuthorDetails
4. Refactor BookShop to reference AUTHOR_ID
5. Verify normalized tables
