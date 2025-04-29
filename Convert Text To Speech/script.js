let speech = new SpeechSynthesisUtterance();
let voices = [];
let voiceSelect = document.querySelector("select");

function populateVoices() {
  voices = window.speechSynthesis.getVoices();
  voiceSelect.innerHTML = "";
  voices.forEach((voice, i) => {
    let option = new Option(voice.name + " (" + voice.lang + ")", i);
    voiceSelect.add(option);
  });
  speech.voice = voices[0];
}

// Kur ngarkohen zërat në telefon ose desktop
window.speechSynthesis.onvoiceschanged = populateVoices;

// Thirr populate edhe direkt (per ca raste)
populateVoices();

// Kur ndërron zërin
voiceSelect.addEventListener("change", () => {
  speech.voice = voices[voiceSelect.value];
});

// Kur klikon "Listen"
document.querySelector("button").addEventListener("click", () => {
  speech.text = document.querySelector("textarea").value;
  window.speechSynthesis.speak(speech);
});
