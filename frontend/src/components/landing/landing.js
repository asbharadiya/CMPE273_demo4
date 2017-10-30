import React, { Component } from 'react';
import Signin from './signin';
import Signup from './signup';

class Landing extends Component {

  constructor(props) {
    super(props);
    this.state = { activeState:'signin' };
    this.handleSignupClick = this.handleSignupClick.bind(this);
    this.handleSigninClick = this.handleSigninClick.bind(this);
  }

  handleSignupClick(){
    this.setState({activeState: 'signup'});
  }

  handleSigninClick(){
    this.setState({activeState: 'signin'});
  }

  render() {
    return (
      <div className="landing-page">
        <header>
        </header>
        <div className="page-content">
          <div className="page-inner-content">
            <div className="form-container">
              {
                this.state.activeState === 'signin' ? (
                  <Signin onSignupClick={this.handleSignupClick}/>
                ):(
                  <Signup onSigninClick={this.handleSigninClick}/>
                )
              }
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default Landing;
