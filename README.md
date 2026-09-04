# Sniper Mecha 1v1

A web-based 1v1 sniper duel game inspired by BGMI's WOW mode.  
Control your mech, aim with your mouse, and eliminate the enemy AI.

## Play Locally

1. Install dependencies: `npm install`
2. Run the server: `npm start`
3. Open `http://localhost:3000` in your browser.

## Deploy to Railway

1. Push this repo to GitHub.
2. On Railway, create a new project from your GitHub repo.
3. Railway will automatically detect the `package.json` and run `npm start`.
4. Your game will be live at the Railway-generated URL.

## Controls

- **Move:** WASD keys
- **Aim:** Mouse
- **Shoot:** Left click

## Game Mechanics

- Sniper rifle with slow fire rate (cooldown ~40 frames) and high damage (35 HP per hit).
- AI opponent with basic behaviours: chase, retreat, strafe.
- Health bars and win/loss conditions.
- Obstacles for cover.

Enjoy!