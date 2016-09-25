# dip-coursework
Coursework for the course Data Intensive Programming

## Some insights
* Dataset contains 180,000 lines
* Length of each line varies from 18 to 24, but if we split each line by space we can guarantee the following:
 * First element always is IP address
 * Fourth element always is date and time
 * Sixth element always is "GET
 * Seventh element always is the link
* Analysis is done on the seventh element of each line (the link) and we found some keywords:
 * add_to_cart with 12736 times
 * view_cart 5582 times
 * contact_us 1648 times
 * checkout 1549 times
* Every line follows a time order from earliest to latest

## Questions
The following questions are addressed and answered
* What are top 10 best - selling products?
 * Nike Men's Dri-FIT Victory Golf Polo with 158 sales
 * Nike Men's CJ Elite 2 TD Football Cleat 156
 * adidas Kids' RG III Mid Football Cleat 155
 * Perfect Fitness Perfect Rip Deck 152
 * Field & Stream Sportsman 16 Gun Fire Safe 106
 * Diamondback Women's Serene Classic Comfort Bi 98
 * Under Armour Girls' Toddler Spine Surge Runni 94
 * O'Brien Men's Neoprene Life Vest 86
 * Nike Men's Free TR 5.0 TB Training Shoe 83
 * Columbia Men's PFG Anchor Tough T-Shirt 80
* What are top 10 most browsed products?
 * Perfect Fitness Perfect Rip Deck 1926
 * adidas Kids' RG III Mid Football Cleat 1793
 * Nike Men's Dri-FIT Victory Golf Polo 1780
 * Nike Men's CJ Elite 2 TD Football Cleat 1757
 * Pelican Sunstream 100 Kayak 1104
 * O'Brien Men's Neoprene Life Vest 1084
 * Diamondback Women's Serene Classic Comfort Bi 1059
 * Field & Stream Sportsman 16 Gun Fire Safe 1028
 * Nike Men's Free 5.0+ Running Shoe 1004
 * Under Armour Hustle Storm Medium Duffle Bag 939
* What are the most browsing hours
 * 21 with 31307 requests
 * 20 30225
 * 22 29288
 * 23 21371
 * 19 19995
 * 18 10303
 * 11 8589
 * 17 6771
 * 12 5563
 * 10 5008
 * 16 4013
 * 13 2788
 * 15 2769
 * 14 2010
