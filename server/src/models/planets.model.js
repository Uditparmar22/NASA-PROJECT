const fs = require('fs');
const path = require('path');
const { rejects } = require('assert');
const { parse } = require('csv-parse');

const planets = require('./planets.mongo')
// const habitablePlanet = []
// parse();
function isHabitablePlanet(planet){
    return planet['koi_disposition'] === 'CONFIRMED'
    && planet['koi_insol'] > 0.36 && planet['koi_insol'] < 1.11
    && planet['koi_prad'] < 1.6
}

function loadPlanetsData(){
    return new Promise((resolve, reject)=>{
       
      fs.createReadStream(path.join(__dirname, '..', '..', 'data','kepler_data.csv'))
      .pipe(parse({
         comment: '#',
         columns: true,
        }))
      .on('data',async (data)=>{
        if(isHabitablePlanet(data)){
          //TODO:Replace below create with insert + update = upsert
          savePlanet(data)
        }
       })
      .on('error', (err)=>{
        console.log(err);
        reject(err)
       })
      .on('end', async()=>{ 
        const countPlanetFound =(await getAllPlanets()).length;
        console.log(`${countPlanetFound} habitable planets found!`);
        resolve(); 
       }); 
        
    });
}
async function getAllPlanets(){
  return await planets.find({},{
    '_id':0, '__v':0
  });
}

async function savePlanet(planet){

  try {
    await planets.updateOne({
      kepler_name: planet.kepler_name,
    },{
      kepler_name:planet.kepler_name
    },{
      upsert:true
    });
  } catch (error) {
    console.error(`could not save planet  ${error}`);
  }

}

 module.exports= {
   loadPlanetsData,
   getAllPlanets,
 };