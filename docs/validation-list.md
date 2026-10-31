# Validation list (pre-registered, PRD §5.6)

**Committed:** 2026-10-12, before any RQ1 (bridge) code or result existed.
**Machine-readable copy:** [`config/validation_artists.csv`](../config/validation_artists.csv) (`validation.artists_file`), read by F4.6.

PRD §5.6 asks for a list of artists widely known for cross-genre collaboration, fixed **before** anyone looks at RQ1 results. The report then states where these artists rank. Fixing the list first stops us from choosing examples after seeing which artists score well.

## The list

| # | Artist | MBID | Why it is expected to bridge genres |
|---:|---|---|---|
| 1 | Quincy Jones | `5803c81e-739a-4057-9a5c-cf84e55db630` | jazz arranger turned pop, soul and film-score producer (Sinatra, Basie, Michael Jackson) |
| 2 | Herbie Hancock | `27613b78-1b9d-4ec3-9db5-fa0743465fdd` | jazz pianist across funk, electronic, pop and world collaborations |
| 3 | Rick Rubin | `07aebfa0-55d6-47e0-a284-12330e3eae0d` | producer for hip hop, metal, rock and country (Run-DMC, Slayer, Johnny Cash) |
| 4 | Brian Eno | `ff95eb47-41c4-4f7f-a104-cdc30f02e872` | ambient pioneer producing rock and pop (Talking Heads, U2, Coldplay, Bowie) |
| 5 | Nile Rodgers | `c6d571dd-c0ae-4ac8-9500-780b1b9b25e5` | disco and funk guitarist/producer for rock, pop and electronic acts |
| 6 | Pharrell Williams | `149f91ef-1287-46da-9a8e-87fee02f1471` | hip hop and R&B producer working with pop, rock and electronic artists |
| 7 | Timbaland | `daa09819-5da5-4c7a-8bef-eb372bb27ff1` | hip hop/R&B producer for pop and rock artists |
| 8 | Mark Ronson | `c3c82bdc-d9e7-4836-9746-c24ead47ca19` | producer bridging soul, pop, funk and rock |
| 9 | Damon Albarn | `7d7a5fdd-0d04-4c36-8bee-906feeae239c` | Blur/Gorillaz; collaborations with hip hop and Malian musicians |
| 10 | Ry Cooder | `a4e2650b-938b-4b72-95bc-6e229cd34601` | roots guitarist across blues, folk, Cuban and Indian music |
| 11 | Béla Fleck | `e1a889b7-5e45-447f-baa0-b9c6cbc0d39f` | banjo across bluegrass, jazz, classical and African music |
| 12 | Yo‐Yo Ma | `62fe877d-b7a5-43e8-952e-2610d7614bff` | classical cellist in bluegrass, tango, Silk Road and film collaborations |
| 13 | Santana | `9a3bf45c-347d-4630-894d-7cf3e8e0b632` | Latin rock band with pop, hip hop and R&B guests (*Supernatural*) |
| 14 | Diplo | `a56bd8f9-8ef8-4d63-89a4-794ed1360dd2` | electronic producer with pop, hip hop, reggae and dancehall artists |
| 15 | Willie Nelson | `668fd73c-bf54-4310-a139-305517f05311` | country singer with duets across pop, rock, reggae, jazz and hip hop |

## How the list was made, and deviations from PRD §5.6

- **Chosen by Claude Code on request** (2026-10-12), not from two independent lists as §5.6 asks, so that P4 could close on schedule. The choice came from general knowledge of these artists' careers, not from project outputs ([D-037](decisions.md)).
- **What had been seen before choosing.**
  - Seen: the dev-sample top-20 lists of F4.1 (degree, PageRank, betweenness) and the F4.2 community examples. Herbie Hancock appeared there: #9 by betweenness in the first F4.1 sample run, and on an F4.5 example path.
  - Not seen: no genre-diversity value, bridge score or bridge ranking existed yet.
- **MBIDs** were looked up by exact name in the full graph's `nodes_artist` (name, type, area and recording count only). Where a name had several artists, the one known for the collaborations was chosen: the band **Santana** (4,222 recordings) rather than Carlos Santana the person, because the *Supernatural* collaborations are credited to the band. **Yo‐Yo Ma** is written with a Unicode hyphen (U+2010) in MusicBrainz.
- **Never edit this list after F4.3 results exist.** A change would need a new decision entry and must be reported as a post-hoc change.
