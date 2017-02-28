# customer-lifetime
simple LTV calculation

Here is a simple version for reading and writing local files according to the limited resources.
When the data size is huge and considering the performance, extracting and updating data streams in real-time will solve the need of  large amount of memory to run this kind of analysis process.

Put one event in one line for the conveniet of read input.
Output customer_id and lifetime_value pairs rather than using customer names, because it is easier for the following use, such as extract customer info from database and update any table using customer_id as a key.
