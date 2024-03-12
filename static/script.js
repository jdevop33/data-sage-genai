document.addEventListener("DOMContentLoaded", function() {
    document.getElementById("chat-form").addEventListener("submit", function(event) {
        event.preventDefault(); // Prevent the form from submitting via HTTP
        let userInput = document.getElementById("chat-input");
        let message = userInput.value.trim();


        if (message) {
            addMessage("user", message);
            fetch("/ask", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ question: message }),
            })
            .then(response => response.json())
            .then(data => {
                if (data.response) { // Handle response based on updated Flask app structure
                    addMessage("bot", data.response); // Display bot response
                } else {
                    addMessage("bot", "Sorry, I couldn't process that."); // Fallback message
                }
            })
            .catch(error => {
                console.error("Error:", error);
                addMessage("bot", "An error occurred."); // Show error message in chat
            });
        }
        userInput.value = ""; // Clear input field after sending
    });
});


// Function to add user or bot message to the chat box
function addMessage(sender, message) {
    let chatBox = document.getElementById("chat-box");
    let messageDiv = document.createElement("div");
    let label = document.createElement("div");
    label.textContent = sender.charAt(0).toUpperCase() + sender.slice(1) + ":";
    label.style.fontWeight = "bold";


    messageDiv.classList.add("message", `${sender}-message`);
    messageDiv.textContent = message;


    let containerDiv = document.createElement("div");
    containerDiv.appendChild(label);
    containerDiv.appendChild(messageDiv);


    chatBox.appendChild(containerDiv);
    chatBox.scrollTop = chatBox.scrollHeight; // Scroll to bottom
}