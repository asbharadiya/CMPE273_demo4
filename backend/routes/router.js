var multer = require('multer');

var upload = multer({storage: multer.memoryStorage()});

module.exports = function(router,passport) {

	var auth = require('./auth')(passport);
	var asset = require('./asset');

	router.post('/api/signin', auth.signin);
	router.post('/api/signup', auth.signup);
	router.post('/api/logout', auth.logout);
	router.get('/api/check_session', isAuthenticated, auth.checkSession);

	router.post('/api/add_asset', isAuthenticated, upload.single('file'), asset.addAsset);
	router.get('/api/get_assets',asset.getAssets);
	
	function isAuthenticated(req, res, next) {
		if(req.session.passport && req.session.passport.user._id) {
			next();
	  	} else {
			res.status(401).send();
		}
	}

}