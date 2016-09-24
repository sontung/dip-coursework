# dip-coursework
Coursework for the course Data Intensive Programming

## Some insights
* Dataset contains 180,000 lines
* Length of each line varies from 18 to 24, but if we split each line by space we can guarantee the following:
 * First element always is IP address
 * Sixth element always is "GET
 * Seventh element always is the link
* Analysis is done on the seventh element of each line (the link) and we found some keywords:
 * add_to_cart with 12736 times
 * view_cart 5582 times
 * contact_us 1648 times
 * checkout 1549 times
