
const initialState= {
  isLogged: undefined,
  assets: []
}

const reducer = (state = initialState, action) => {
  switch (action.type) {
    case "SESSION_ACTIVE" :
      return {...state,isLogged:true};
    case "SESSION_INACTIVE" :  
      return {...state,isLogged:false};
    case "GET_ASSETS_SUCCESS" :
      return {
        ...state,
        assets:action.data,
        uploadFileSuccess:undefined
      };
    case "GET_ASSETS_FAILURE" :  
      return state;
    case "ADD_ASSET_SUCCESS" :
      return {...state,uploadFileSuccess:true};
    case "ADD_ASSET_FAILURE" :  
      return {...state,uploadFileSuccess:false};
    default : 
      return state;
  }
};

export default reducer;
