const api = process.env.REACT_APP_CONTACTS_API_URL || 'http://localhost:3001'

const headers = {
    'Accept': 'application/json'
};

export const addAsset = (formData) =>
	fetch(api+'/api/add_asset', {
	    method: 'POST',
        credentials: 'include',
        body: formData
	}).then(res => {
		return res.json();
	}).catch(error => {
        return error;
    }); 

export const getAssets = () =>
	fetch(api+'/api/get_assets', {
	    method: 'GET',
        headers: {
	    	...headers,
            'Content-Type': 'application/json'
        },
        credentials: 'include'
	}).then(res => {
		return res.json();
	}).catch(error => {
        return error;
    }); 
