# Request and Reply Design

## Definitions

- **Request Subject** A Publish and Subscribe subject that does not contain a wild card.
- **Derived Subject** A subject that is derived by prepending and/or appending parts to a **Request Subject**. The derived subject should observe the length limits imposed on a Publish and Subscribe subject.
    - **Derived Request Subject** For a **request subject**, the **Derived Request Subject** is `_request.<request_subject>`.
    - **Derived Reply Subject** For a **request subject**, the **Derived Reply Subject** is `_reply.<request_subject>.<unique_id>`.
        - The **unique_id** is a random character string containing only 0-9, A-Z and a-z.
        - It should be 32 characters long.
        - It must come from a cryptographically secure random source (e.g. Python's `secrets` module), so that it cannot be guessed.
- **Valid Reply Subject** A subject that has the form of a **Derived Reply Subject**:
    - It contains no wild card.
    - Its first sub-string is `_reply` and its last sub-string is a **unique_id** (32 characters, only 0-9, A-Z and a-z), with at least one sub-string between them.
- **Parameter Checks** **Accept Request**, **Stop Accept Request**, **Send Request** and **Send Reply** check the parameters they are given before doing anything else. If a check fails, report an error to the caller and change nothing: no subscription is made or removed, no timer is started and nothing is published.
    - A **request subject** must not contain a wild card.
    - Every derived subject the operation uses must fit the Publish and Subscribe subject length limit (65535 bytes once UTF-8 encoded, see `data-format.md`):
        - **Accept Request** and **Stop Accept Request** use the **Derived Request Subject**, so the **request subject** can be at most 65526 bytes.
        - **Send Request** uses both derived subjects; the **Derived Reply Subject** is the longer, so the **request subject** can be at most 65495 bytes.
    - The **timeout** given to **Send Request** must be greater than or equal to 0.
    - The **reply subject** given to **Send Reply** must be a **Valid Reply Subject**. This keeps a request from making its acceptors publish to an ordinary Publish and Subscribe subject (see **Reserved Subjects**).

## Reserved Subjects

- Subjects whose first sub-string is `_request` or `_reply` are reserved for request and reply. Publish and Subscribe users should not publish or subscribe to them directly.
- The reservation is a convention only; nothing enforces it. A Publish and Subscribe subscriber whose subject has a wild card can still match reserved subjects, e.g. `*.x` matches `_request.x` and so receives every request sent to `x`.

## Messages

The wire formats are defined in "Request and Reply Wire Format" in `data-format.md`. Both messages are carried as the payload of a Publish and Subscribe publish message.

- **Request Message** Published to a **Derived Request Subject**. It carries:
    - The **reply subject**, which is the requestor's **Derived Reply Subject**.
    - The **request payload**, which could be empty.
- **Reply Message** Published to a **reply subject**. Its payload is the **reply payload** as-is, which could be empty.

## Callbacks

- Every callback given to the class (**received request callback**, **received reply callback**, connection error callback) can be either synchronous or asynchronous.
- If a callback raises an error, log the error with its details and carry on. The error is not reported to the caller of the operation that triggered the callback, and the connection is kept.
    - A failing **received request callback** does not stop the other **received request callbacks** for the same request from being called.
    - A failing **received reply callback** does not end the wait: it counts as returning a false value, and later replies are still delivered.
    - A failing **received reply callback** in the call with **timed out** set to true does not stop **Close** or a connection error from ending the other requests' waits.
- Callbacks run on the Publish and Subscribe client's receive loop: the client handles the next received message only once the callback has returned (see Message Processing in `publish-subscribe.md`). A callback that takes long therefore delays everything else the client receives, including replies to its own requests. Long work should be started as a separate task rather than done or awaited inside the callback.

## Request and Reply Client Class

- Use the Publish and Subscribe Client class for requesting and replying.

- **Instantiation** Create an instance of the class.
    - Create an instance of the Publish and Subscribe client. It takes no parameters; the upstream node is given to **Connect**.
- **Connect** Connect to the upstream node, as the Publish and Subscribe client does.
    - Parameters:
        - The IP address and port number of the upstream node.
            - The defaults are the same as the Base Layer Client's.
- **Register Connection Error Callback** Provide a function to call when there is an error with the connection to the upstream node.
    - The default is the same as the Publish and Subscribe client's.
    - The class registers its own connection error callback on the Publish and Subscribe client. When the connection fails, it:
        - Ends the wait of every request whose wait has not ended yet, and calls their **received reply callback**, the same way as **Close** does.
        - Then calls the registered connection error callback.
- **Close** Close the connection to the upstream node.
    - Before closing the connection, for every request whose wait has not ended yet (see **End of the wait** in **Send Request**):
        - End the wait.
        - Call its **received reply callback** with its **Derived Reply Subject**, an empty **reply payload**, and **timed out** set to true, the same way as **Upon timeout**.
            - The return value of the **received reply callback** is ignored.

- **Accept Request**
    - Parameters:
        - A **request subject** to accept requests on.
        - A function that is the **received request callback**.
            - The function takes the following parameters:
                - The **reply subject** to reply to the requestor.
                - The **request payload** from the requestor.
    - Actions:
        - If the same **received request callback** is already registered for the same **request subject**, do nothing: the callback is called once per request, not once per registration.
        - Subscribe to the **Derived Request Subject**, with a new subscription callback made for this **received request callback**.
            - Remember the subscription callback under the pair (**request subject**, **received request callback**), so that **Stop Accept Request** can unsubscribe that exact function.
            - The subscription callback will be a function that does the following:
                - Decode the payload given to the subscription callback as a **Request Message**, to get the **reply subject** and the **request payload**.
                    - If the payload is a malformed Request Message (see `data-format.md`), log it and ignore it. The **received request callback** is not called.
                    - Likewise, if the **reply subject** is not a **Valid Reply Subject** whose sub-strings between `_reply` and the **unique_id** are the **request subject**, log the request and ignore it.
                - The **request payload** could be empty.
                - Call the **received request callback** with the **reply subject** and the **request payload**.
    - Notes (intended behaviour):
        - Every **received request callback** accepting requests on a **request subject**, in every client, receives every request sent to it. A requestor can therefore get several replies; it ends the wait when it has the replies it wants.
        - A client that accepts requests on a **request subject** also receives its own requests sent to that subject (see Publish in `publish-subscribe.md`).

- **Stop Accept Request**
    - Parameters:
        - A **request subject** to stop accepting requests on.
        - A function that is the registered **received request callback**.
    - Actions:
        - If no subscription callback is remembered under the pair (**request subject**, **received request callback**), do nothing.
        - Otherwise, unsubscribe the remembered subscription callback from the **Derived Request Subject**, and forget it.
            - Other **received request callbacks** accepting requests on the same **request subject** are not affected.

- **Send Request**
    - Parameters:
        - A **request subject** to send the request to.
        - A **request payload** to send with the request.
            - This could be empty.
        - A function that is the **received reply callback**.
            - The function takes the following parameters:
                - The **reply subject** that is used to receive this reply.
                - The **reply payload**.
                - The **timed out** flag: true if this call carries no reply because the wait ended by the timeout, by **Close**, or by a connection error; otherwise false.
        - A **timeout** in seconds.
            - Default is 5 minutes (which is 5 * 60 = 300 seconds).
            - A timeout value of 0 means wait forever.
            - A timeout value must be greater than or equal to 0.
    - Actions:
        - Subscribe to the **Derived Reply Subject**.
            - The subscription callback will be a function that does the following:
                - Use the **actual subject** received in the subscription callback as the **reply subject**.
                - Use the **payload** received in the subscription callback as the **reply payload**.
                - If the wait has already ended (see **End of the wait** below), do nothing.
                - Call the **received reply callback** with the **reply subject**, the **reply payload**, and **timed out** set to false.
                    - If the call to the **received reply callback** returns a true value, end the wait, unless it has already ended meanwhile.
        - Start the timer for the **timeout**.
            - If the **timeout** is 0, no timer is started: the wait does not end by timeout, and **Upon timeout** never happens.
            - The timer is started before the request is published: the requestor's own Accept Request callbacks, and so a reply, can run while the request is being published (see Publish in `publish-subscribe.md`).
        - Publish a **Request Message**, with the **Derived Reply Subject** as its **reply subject** and the given **request payload**, to the **Derived Request Subject**.
    - **Upon failure** If subscribing or publishing fails (e.g. the client is not connected, or the connection fails while sending):
        - End the wait, unless it has already ended. The subscription is removed locally even if the upstream node cannot be told.
        - Report the error to the caller. Nothing is returned, and the **received reply callback** is not called with **timed out** set to true for this request.
        - While Send Request has not returned yet, a connection error (and **Close**) does not end this request's wait or call its **received reply callback**; the failure is reported only through the error above. This keeps a single failure from being reported twice.
    - **Closed meanwhile** If **Close** or a connection error happened while Send Request had not returned yet, but subscribing and publishing still succeeded:
        - Once publishing has finished, end the wait (unless it has already ended) and call the **received reply callback** with the **Derived Reply Subject**, an empty **reply payload**, and **timed out** set to true, the same way **Close** or the connection error would have.
        - Then return the **Derived Reply Subject** as usual.
        - Without this, the request would be left waiting on a closed client: until its timeout, or forever with a **timeout** of 0.
    - Returns:
        - The **Derived Reply Subject**, which identifies this request (e.g. to **Cancel Request**).
            - Its **unique_id** makes it different for every request, even for requests sent to the same **request subject** with the same **received reply callback**.
            - The wait may already have ended by the time Send Request returns, e.g. when the requestor's own Accept Request callback replied while the request was being published.
    - **Upon timeout**
        - If the wait has already ended, do nothing.
        - End the wait.
        - Call the **received reply callback** with the **Derived Reply Subject**, an empty **reply payload**, and **timed out** set to true.
            - The return value of the **received reply callback** is ignored.
    - **End of the wait**
        - The wait for replies to a request ends exactly once, by whichever comes first: the **received reply callback** returning a true value, the timeout, **Cancel Request**, **Close**, a connection error (including one that happened while Send Request had not returned yet, see **Closed meanwhile**), or a failure in Send Request (see **Upon failure**).
        - Ending the wait does the following:
            - Mark the wait as ended.
            - Unsubscribe from the **Derived Reply Subject**.
                - If the upstream node cannot be told because the connection has failed, the subscription is still removed locally and the error is ignored.
            - Cancel the timer, if there is one and it has not fired.
        - Once the wait has ended, the **received reply callback** is not called again, except for the single call with **timed out** set to true when the wait ends by the timeout, by **Close**, or by a connection error.
            - For example, if the timeout fires while an asynchronous **received reply callback** is still running, the wait ends by the timeout; when that running callback later returns a true value, nothing more happens.

- **Cancel Request**
    - Parameters:
        - The **Derived Reply Subject** returned by the **Send Request** of the request to cancel.
    - Actions:
        - If there is no such request, or its wait has already ended, do nothing.
        - Otherwise, end the wait (see **End of the wait** in **Send Request**).
        - The **received reply callback** is not called: the caller of Cancel Request already knows the wait has ended.

- **Send Reply**
    - Parameters:
        - The **reply subject** that is the subject to publish the payload to.
        - The **reply payload**.
    - Actions:
        - Publish the **reply payload** to the **reply subject**.
