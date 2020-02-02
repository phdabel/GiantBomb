import giantbomb.giantbomb as giantbomb
import os

gb = giantbomb.Api(os.environ['API_KEY'],'gfBOT')

x = gb.get("/game/3030-1/", {}, False)
print(x.get('franchises',[]))

