## Prompt Engineering — Övnings-Outline

### Grundläggande tips

- **Var specifik**: Beskriv mål, avgränsningar, målgrupp, domän, formatkrav och vad som inte ska göras.
- **Använd tekniska beskrivningar**: Ange terminologi, API‑er, bibliotek, versionskrav, datatyper och I/O‑format.
- **Ge prompten kontext**: Lägg till syfte, roll, målpublik, exempeldata och miljöantaganden.
- **Använd markdown**: Strukturera med rubriker (##, ###), listor och **fetstil** för krav.
- **Dela upp innehållet**: Strukturera svar i sektioner med markdown eller med XML‑taggar enligt behov.

### Tekniker och övningar

#### 1) Styr längd och format

- **Övningar**:
  - Be modellen ge ett svar på max 120 ord, sedan samma svar i 3 nivåer: 50/120/300 ord.
  - Be om output i strikt JSON med fast schema och validera i en JSON‑validator.

#### 2) Dela upp i mindre delar

- **Mål**: Bryta ner komplexa problem i hanterbara steg och leverera delresultat sekventiellt.
- **Övningar**:
  - Be modellen först göra en delplan (rubriker), därefter leverera endast steg 1, invänta “fortsätt” för nästa steg.
  - Använd checklista där varje del levereras med tydliga kriterier för “klart”.

#### 3) Rollspels‑prompting

- **Mål**: Få svar anpassade efter expertroll, ton och ansvar.
- **Övningar**:
  - Be modellen agera som någon på en LIA-intervju (använd voice-mode)
  - Be modellen agera “produktägare” vs “CTO” och jämför fokus/nytta.
  - Låt modellen vara “strikt kod-granskare som pekar ut risker och frågetecken. (kanske med era projekt ni nyss lämnade in)

#### 4) Meta‑prompting

- **Mål**: Låta modellen skapa/improva promptar och processer för att nå bättre resultat.
- **Övningar**:
  - “Skriv en förbättrad version av min prompt, med tydligare mål, format och testbara kriterier.”
  - "Jag kommer att ge dig en prompt. Hjälp mig förbättra den genom att ställa frågor och hjälp mig sedan förbättra min ursprungliga prompt"

#### 5) One‑shot vs. few‑shot

- **Mål**: Förstå när ett enstaka exempel räcker och när flera behövs för generalisering.
- **Övningar**:
  - Be en modell skriva e

### Ytterligare övning: Meta‑prompting för miniprojekt (imorgon)

Imorgon arbetar ni i era nya grupper (individuellt) med ett kortare projekt. Välj antingen en egen idé eller någon av nedan. Använd dagens tekniker för att:

- **Göra research**
- **Skapa en plan**
- **Bryta ner arbetet** (dela upp idén i mindre bitar så det blir enklare att jobba del för del)

#### Alternativ på projektidéer

- **Bygg ett spel**

  - Bolla en idé med modellen, håll beskrivning och scope kort.

- **Bygg en trummaskin**

  - Krav: minst 4 ljud (kick, snare, hihat, crash).
  - UI: tilltalande gränssnitt; lägg gärna till cutoff‑filter, reverb, delay.
  - Tips: använd `Tone.js` för ljuduppspelning.
  - Bolla funktioner/UX med modellen.

- **Bygg ett minimalt men fungerande socialt nätverk**

  - Funktioner: registrering/inloggning, uppdatera profil, posta texter.
  - Backend: t.ex. MongoDB + Mongoose.
  - Miljö: lägg `connection string` (inkl. `database name`) i `.env` och visa för modellen vid behov.

- **Bygg en portfolio‑sida**
  - Rollspel: låt modellen vara “beställaren”. Samla krav, få feedback, iterera.
  - Bildmaterial: använd text‑till‑bild‑modell för att ta fram resurser.
  - UI: prova animationsbibliotek (t.ex. `anime.js`) för parallax/animationer.
